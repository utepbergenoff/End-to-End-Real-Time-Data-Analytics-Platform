"""Command-line entry points for the classroom analytics platform."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import platform
import sqlite3
import sys
import time
import uuid

import joblib
import psutil
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from core import Processor, ROOMS, START, evaluate_anomalies, generate, train_and_evaluate

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
ARTIFACTS = ROOT / "artifacts"
DB = DATA / "classroom.sqlite"
BROKER = "localhost:9092"
TOPICS = ("classroom.raw", "classroom.processed", "classroom.windows")


def connect_db():
    DATA.mkdir(exist_ok=True)
    db = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript("""
    CREATE TABLE IF NOT EXISTS raw (event_id TEXT PRIMARY KEY, timestamp TEXT, room_id TEXT,
      payload TEXT NOT NULL, received_at TEXT);
    CREATE TABLE IF NOT EXISTS processed (event_id TEXT PRIMARY KEY, timestamp TEXT, room_id TEXT,
      temperature_c REAL, humidity_pct REAL, co2_ppm REAL, occupancy INTEGER,
      rolling_co2_mean REAL, rolling_co2_std REAL, anomaly INTEGER, high_co2_alert INTEGER, prediction_co2 REAL,
      cleaned_fields TEXT, run_id TEXT, latency_ms REAL, stored_at REAL);
    CREATE TABLE IF NOT EXISTS windows (room_id TEXT, window_start TEXT, window_end TEXT,
      event_count INTEGER, avg_co2_ppm REAL, avg_temperature_c REAL, anomaly_count INTEGER,
      PRIMARY KEY (room_id, window_start));
    CREATE TABLE IF NOT EXISTS stream_clock (id INTEGER PRIMARY KEY CHECK(id=1), last_event_time TEXT);
    """)
    if "high_co2_alert" not in [row[1] for row in db.execute("PRAGMA table_info(processed)")]:
        db.execute("ALTER TABLE processed ADD COLUMN high_co2_alert INTEGER DEFAULT 0")
    return db


def kafka_producer():
    return Producer({"bootstrap.servers": BROKER, "acks": "all", "retries": 3})


def kafka_consumer(topics, group):
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(list(topics))
    return consumer


def publish(producer, topic, event, *, wait=False):
    delivery = []
    def callback(error, _message):
        delivery.append(error)
    producer.produce(topic, key=event["room_id"],
                     partition=ROOMS.index(event["room_id"]),
                     value=json.dumps(event).encode("utf-8"), on_delivery=callback)
    producer.poll(0)
    if wait:
        while not delivery:
            producer.poll(1)
        if delivery[0]:
            raise KafkaException(delivery[0])


def setup_topics():
    admin = AdminClient({"bootstrap.servers": BROKER})
    for topic in TOPICS:
        try:
            admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(20)
            print("created", topic)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print("exists", topic)


def reserve_event_start(minutes):
    """Reserve non-overlapping simulated time across demo and benchmark runs."""
    db = connect_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT last_event_time FROM stream_clock WHERE id=1").fetchone()
        if row:
            start = datetime.fromisoformat(row[0].replace("Z", "+00:00")) + timedelta(minutes=1)
        else:
            last = db.execute("SELECT MAX(timestamp) FROM raw").fetchone()[0]
            start = (datetime.fromisoformat(last.replace("Z", "+00:00")) + timedelta(minutes=1)) if last else START
        end = start + timedelta(minutes=minutes-1)
        db.execute("INSERT OR REPLACE INTO stream_clock VALUES (1,?)", (end.isoformat().replace("+00:00", "Z"),))
        db.commit()
        return start
    finally:
        db.close()


def produce(minutes, rate, seed=42, run_id=None):
    producer = kafka_producer()
    run_id = run_id or "demo-" + uuid.uuid4().hex[:8]
    start = reserve_event_start(minutes)
    interval = 0 if rate <= 0 else 1 / rate
    next_send = time.perf_counter()
    count = 0
    for event in generate(minutes, seed=seed, start=start):
        event["event_id"] = run_id + "-" + event["event_id"]
        event["run_id"] = run_id
        event["sent_at_epoch"] = time.time()
        publish(producer, "classroom.raw", event)
        count += 1
        next_send += interval
        if interval:
            time.sleep(max(0, next_send - time.perf_counter()))
    if producer.flush(60):
        raise RuntimeError("Some Kafka events were not delivered")
    print(f"sent {count} events at target {rate:g} events/s")


def process_stream():
    model_path = ARTIFACTS / "co2_model.joblib"
    if not model_path.exists():
        raise SystemExit("Train first: python app.py train")
    processor = Processor(joblib.load(model_path))
    producer = kafka_producer()
    print("processor ready", flush=True)
    try:
        while True:
            consumer = kafka_consumer(["classroom.raw"], "classroom-processors")
            try:
                while True:
                    message = consumer.poll(1)
                    if message is None:
                        continue
                    if message.error():
                        raise KafkaException(message.error())
                    event = json.loads(message.value().decode("utf-8"))
                    try:
                        result = processor.process(event)
                    except ValueError as error:
                        print(f"rejected {event.get('event_id', '?')}: {error}", file=sys.stderr)
                    else:
                        publish(producer, "classroom.processed", result, wait=True)
                    consumer.commit(message=message, asynchronous=False)
            except KafkaException as error:
                print(f"consumer reconnecting: {error}", file=sys.stderr, flush=True)
                time.sleep(1)
            finally:
                consumer.close()
    finally:
        producer.flush(10)


def store_stream():
    db = connect_db()
    print("storage ready", flush=True)
    try:
        while True:
            consumer = kafka_consumer(TOPICS, "classroom-storage")
            try:
                while True:
                    message = consumer.poll(1)
                    if message is None:
                        continue
                    if message.error():
                        raise KafkaException(message.error())
                    e = json.loads(message.value().decode("utf-8"))
                    if message.topic() == "classroom.raw":
                        db.execute("INSERT OR IGNORE INTO raw VALUES (?,?,?,?,?)",
                                   (e["event_id"], e["timestamp"], e["room_id"],
                                    json.dumps(e), datetime.now(timezone.utc).isoformat()))
                    elif message.topic() == "classroom.processed":
                        now = time.time()
                        latency = (now - e["sent_at_epoch"]) * 1000 if "sent_at_epoch" in e else None
                        db.execute("""INSERT OR REPLACE INTO processed
                          (event_id,timestamp,room_id,temperature_c,humidity_pct,co2_ppm,occupancy,
                           rolling_co2_mean,rolling_co2_std,anomaly,high_co2_alert,prediction_co2,
                           cleaned_fields,run_id,latency_ms,stored_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                   (e["event_id"], e["timestamp"], e["room_id"], e["temperature_c"],
                                    e["humidity_pct"], e["co2_ppm"], e["occupancy"], e["rolling_co2_mean"],
                                    e["rolling_co2_std"], int(e["anomaly"]), int(e.get("high_co2_alert", False)), e["prediction_co2"],
                                    json.dumps(e["cleaned_fields"]), e.get("run_id"), latency, now))
                    elif message.topic() == "classroom.windows":
                        db.execute("INSERT OR REPLACE INTO windows VALUES (?,?,?,?,?,?,?)",
                                   (e["room_id"], e["window_start"], e["window_end"], e["event_count"],
                                    e["avg_co2_ppm"], e["avg_temperature_c"], e["anomaly_count"]))
                    db.commit()
                    consumer.commit(message=message, asynchronous=False)
            except KafkaException as error:
                print(f"storage consumer reconnecting: {error}", file=sys.stderr, flush=True)
                time.sleep(1)
            finally:
                consumer.close()
    finally:
        db.close()


HTML = """<!doctype html><html><head><meta charset='utf-8'><title>Classroom Sensor Analytics</title>
<style>body{font:16px Arial,sans-serif;background:#f4f7fb;color:#18304a;margin:28px;max-width:1200px}h1{margin-bottom:3px}
.sub{color:#516479}.cards{display:flex;gap:12px;flex-wrap:wrap;margin:22px 0}.card{background:white;padding:18px;border-radius:10px;min-width:195px;box-shadow:0 2px 8px #dce4ee}
.big{font-size:26px;font-weight:bold;margin:9px 0}.bad{color:#b21e2b}table{border-collapse:collapse;background:white;width:100%;margin:12px 0 24px}
td,th{padding:9px;border-bottom:1px solid #e5ebf2;text-align:left}svg{background:white;width:100%;height:230px;border-radius:10px}
</style></head><body><h1>Classroom Sensor Analytics</h1><div class='sub'>Synthetic readings · simulated event time · refreshes every 2 seconds</div>
<div id='status'></div><div id='cards' class='cards'></div><h2>CO₂ history</h2><svg id='chart' viewBox='0 0 900 230'></svg>
<h2>Recent alerts</h2><table><thead><tr><th>Event time (UTC)</th><th>Room</th><th>Type</th><th>CO₂ ppm</th><th>Temperature °C</th></tr></thead><tbody id='alerts'></tbody></table>
<h2>Spark five-minute windows</h2><table><thead><tr><th>Window start (UTC)</th><th>Room</th><th>Events</th><th>Average CO₂</th><th>Anomalies</th></tr></thead><tbody id='windows'></tbody></table>
<script>
const esc=x=>String(x??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
async function refresh(){try{const d=await (await fetch('/api')).json();document.getElementById('status').textContent=`${d.total} processed readings · ${d.raw} raw · ${d.window_count} Spark windows`;
document.getElementById('cards').innerHTML=d.current.map(x=>`<div class='card'><b>${esc(x.room_id)}</b><div class='big ${x.anomaly?'bad':''}'>${esc(x.co2_ppm)} ppm</div><div>${esc(x.temperature_c)} °C · ${esc(x.humidity_pct)}% humidity</div><div>Next CO₂: ${esc(x.prediction_co2)} ppm</div><small>${esc(x.timestamp)}</small></div>`).join('');
document.getElementById('alerts').innerHTML=d.alerts.map(x=>`<tr><td>${esc(x.timestamp)}</td><td>${esc(x.room_id)}</td><td>${x.anomaly?'Sudden spike':'High CO₂'}</td><td>${esc(x.co2_ppm)}</td><td>${esc(x.temperature_c)}</td></tr>`).join('');
document.getElementById('windows').innerHTML=d.windows.map(x=>`<tr><td>${esc(x.window_start)}</td><td>${esc(x.room_id)}</td><td>${esc(x.event_count)}</td><td>${Number(x.avg_co2_ppm).toFixed(1)}</td><td>${esc(x.anomaly_count)}</td></tr>`).join('');
const colors={A101:'#147d79',B202:'#d17a29',C303:'#7356ad'};let svg='<line x1="40" y1="195" x2="880" y2="195" stroke="#9caebf"/>';
for(const [room,rows] of Object.entries(d.history)){if(!rows.length)continue;const pts=rows.map((x,i)=>`${40+i*840/Math.max(1,rows.length-1)},${195-Math.min(170,Math.max(0,(x.co2_ppm-350)*.17))}`).join(' ');svg+=`<polyline fill="none" stroke="${colors[room]}" stroke-width="3" points="${pts}"/><text x="${45+Object.keys(colors).indexOf(room)*120}" y="20" fill="${colors[room]}">${room}</text>`;}document.getElementById('chart').innerHTML=svg;
}catch(e){document.getElementById('status').textContent='Waiting for storage service';}}
refresh();setInterval(refresh,2000);
</script></body></html>"""


def dashboard_data(db):
    db.row_factory = sqlite3.Row
    def rows(sql, params=()):
        return [dict(x) for x in db.execute(sql, params).fetchall()]
    last = db.execute("SELECT run_id FROM processed ORDER BY stored_at DESC LIMIT 1").fetchone()
    run_id = last[0] if last else None
    current = rows("SELECT * FROM processed WHERE run_id=? AND stored_at IN (SELECT MAX(stored_at) FROM processed WHERE run_id=? GROUP BY room_id) ORDER BY room_id", (run_id, run_id)) if run_id else []
    history = {room: rows("SELECT timestamp,co2_ppm FROM processed WHERE room_id=? AND run_id=? ORDER BY stored_at DESC LIMIT 60", (room, run_id)) for room in ROOMS}
    for room in history:
        history[room].reverse()
    return {"raw": db.execute("SELECT count(*) FROM raw").fetchone()[0],
            "total": db.execute("SELECT count(*) FROM processed").fetchone()[0],
            "window_count": db.execute("SELECT count(*) FROM windows").fetchone()[0],
            "current": current, "history": history,
            "alerts": rows("SELECT timestamp,room_id,co2_ppm,temperature_c,anomaly FROM processed WHERE run_id=? AND (anomaly=1 OR high_co2_alert=1) ORDER BY stored_at DESC LIMIT 12", (run_id,)),
            "windows": rows("SELECT * FROM windows ORDER BY window_start DESC LIMIT 15")}


def serve(port=8000):
    connect_db().close()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api":
                db = connect_db()
                try:
                    payload = json.dumps(dashboard_data(db)).encode("utf-8")
                finally:
                    db.close()
                kind = "application/json"
            elif self.path == "/":
                payload, kind = HTML.encode("utf-8"), "text/html; charset=utf-8"
            elif self.path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"dashboard http://127.0.0.1:{port}")
    server.serve_forever()


def benchmark():
    ARTIFACTS.mkdir(exist_ok=True)
    db = connect_db()
    results = []
    for rate in (10, 100, 500):
        run_id = "bench-" + uuid.uuid4().hex[:8]
        expected = 300
        psutil.cpu_percent(interval=None)
        started = time.time()
        produce(minutes=100, rate=rate, run_id=run_id)
        cpu = psutil.cpu_percent(interval=None)
        deadline = time.time() + 90
        while time.time() < deadline:
            count = db.execute("SELECT count(*) FROM processed WHERE run_id=?", (run_id,)).fetchone()[0]
            if count >= expected:
                break
            time.sleep(.5)
        stats = db.execute("SELECT count(*), avg(latency_ms), MIN(stored_at), MAX(stored_at) FROM processed WHERE run_id=?", (run_id,)).fetchone()
        observed = stats[0]
        elapsed = max((stats[3] or time.time()) - started, .001)
        results.append({"target_events_per_second": rate, "expected": expected, "received": observed,
                        "host": f"{platform.system()} {platform.machine()}",
                        "observed_events_per_second": round(observed / elapsed, 2),
                        "mean_end_to_end_latency_ms": round(stats[1], 2) if stats[1] else None,
                        "system_cpu_percent": cpu, "wall_seconds": round(elapsed, 2),
                        "complete": observed == expected})
        print(results[-1])
    db.close()
    (ARTIFACTS / "benchmark.json").write_text(json.dumps(results, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup-topics")
    sub.add_parser("train")
    sub.add_parser("evaluate")
    sub.add_parser("processor")
    sub.add_parser("storage")
    sub.add_parser("benchmark")
    p = sub.add_parser("produce")
    p.add_argument("--minutes", type=int, default=240)
    p.add_argument("--rate", type=float, default=30)
    p = sub.add_parser("dashboard")
    p.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    if args.command == "setup-topics": setup_topics()
    elif args.command == "train": print(json.dumps(train_and_evaluate(ARTIFACTS), indent=2))
    elif args.command == "evaluate": print(json.dumps(evaluate_anomalies(ARTIFACTS), indent=2))
    elif args.command == "processor": process_stream()
    elif args.command == "storage": store_stream()
    elif args.command == "benchmark": benchmark()
    elif args.command == "produce": produce(args.minutes, args.rate)
    elif args.command == "dashboard": serve(args.port)


if __name__ == "__main__":
    main()
