"""Build the report and presentation PDFs from measured project artifacts."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.pdfgen import canvas

from core import generate

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "artifacts"
DATA = ROOT / "data"
BLUE = colors.HexColor("#173b57")
TEAL = colors.HexColor("#147d79")


def read_json(name):
    path = OUT / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def live_counts():
    db_path = DATA / "classroom.sqlite"
    if not db_path.exists():
        return None
    db = sqlite3.connect(db_path)
    try:
        return {name: db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                for name in ("raw", "processed", "windows")}
    finally:
        db.close()


def fmt(number, digits=2):
    return "not measured" if number is None else f"{number:.{digits}f}"


def make_chart():
    rows = list(generate(100))
    fig, ax = plt.subplots(figsize=(9, 3))
    for room, color in (("A101", "#147d79"), ("B202", "#d17a29"), ("C303", "#7356ad")):
        subset = [r for r in rows if r["room_id"] == room]
        ax.plot(range(len(subset)), [r["co2_ppm"] for r in subset], label=room, color=color, linewidth=1.6)
    ax.set(xlabel="Simulated minute", ylabel="CO2 (ppm)", title="Deterministic sample of classroom readings")
    ax.grid(alpha=.2)
    ax.legend(ncol=3)
    fig.tight_layout()
    path = OUT / "sample_series.png"
    fig.savefig(path, dpi=170)
    plt.close(fig)
    return path


def make_architecture():
    fig, ax = plt.subplots(figsize=(11, 3.2))
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 3.2)
    ax.axis("off")
    steps = [(0.1, "Sensor\ngenerator"), (2.0, "Kafka\nraw"), (3.9, "Python\nworkers"),
             (5.8, "Kafka\nprocessed"), (7.7, "Spark\nwindows"), (9.6, "SQLite +\ndashboard")]
    for i, (x, label) in enumerate(steps):
        ax.add_patch(FancyBboxPatch((x, 1.2), 1.25, .9, boxstyle="round,pad=0.12",
                                    facecolor="#e5f3f1" if i in (0, 2, 4) else "#e9eff6",
                                    edgecolor="#147d79", linewidth=1.5))
        ax.text(x + .625, 1.65, label, ha="center", va="center", fontsize=10, color="#173b57")
        if i < len(steps)-1:
            ax.add_patch(FancyArrowPatch((x+1.37,1.65),(steps[i+1][0]-.08,1.65),
                                         arrowstyle="->", mutation_scale=12, color="#617184"))
    ax.text(2.62,.85,"3 room partitions",ha="center",fontsize=9,color="#617184")
    ax.text(4.52,.85,"validate + ML + alerts",ha="center",fontsize=9,color="#617184")
    ax.text(8.32,.85,"5-minute event time",ha="center",fontsize=9,color="#617184")
    ax.text(7.0,.25,"Storage also consumes raw and processed Kafka topics",ha="center",fontsize=9,color="#617184")
    fig.tight_layout()
    path = OUT / "architecture.png"
    fig.savefig(path,dpi=180)
    plt.close(fig)
    return path


def report_page(canvas, doc):
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#617184"))
    canvas.drawString(2 * cm, 1.25 * cm, "Classroom Sensor Analytics | Yerlan Utepbergenov")
    canvas.drawRightString(A4[0] - 2 * cm, 1.25 * cm, str(doc.page))


def build_report(ml, anomaly, bench, chart, architecture, counts):
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="ReportTitleX", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=22,
                              leading=27, textColor=BLUE, spaceAfter=16))
    styles.add(ParagraphStyle(name="ReportH1X", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=16,
                              leading=20, textColor=BLUE, spaceAfter=12))
    styles.add(ParagraphStyle(name="ReportBodyX", parent=styles["BodyText"], fontSize=10.5, leading=15,
                              spaceAfter=9))
    styles.add(ParagraphStyle(name="ReportSmallX", parent=styles["BodyText"], fontSize=8.5, leading=12,
                              spaceAfter=6))
    styles.add(ParagraphStyle(name="ReportHeaderX", parent=styles["ReportSmallX"],
                              textColor=colors.white, fontName="Helvetica-Bold"))
    P = lambda text: Paragraph(text, styles["ReportBodyX"])
    H = lambda text: Paragraph(text, styles["ReportH1X"])
    small = lambda text: Paragraph(text, styles["ReportSmallX"])
    story = []
    def table(rows, widths=None):
        t = Table([[Paragraph(str(c), styles["ReportHeaderX"] if i == 0 else styles["ReportSmallX"])
                    for c in row] for i, row in enumerate(rows)], colWidths=widths, repeatRows=1, hAlign="LEFT")
        t.setStyle(TableStyle([("BACKGROUND", (0,0),(-1,0), BLUE), ("TEXTCOLOR", (0,0),(-1,0), colors.white),
                               ("ROWBACKGROUNDS", (0,1),(-1,-1), [colors.white, colors.HexColor("#f0f5f7")]),
                               ("VALIGN", (0,0),(-1,-1), "TOP"), ("LEFTPADDING", (0,0),(-1,-1), 8),
                               ("RIGHTPADDING", (0,0),(-1,-1), 8), ("TOPPADDING", (0,0),(-1,-1), 7),
                               ("BOTTOMPADDING", (0,0),(-1,-1), 7)]))
        return t
    def page(): story.append(PageBreak())

    story += [Paragraph("End-to-End Real-Time Classroom Sensor Analytics", styles["ReportTitleX"]),
              P("Final technical report | Real-Time Data Analysis | Yerlan Utepbergenov | 17 November 2026"),
              Spacer(1, 18), H("Executive summary"),
              P("This project implements one synthetic classroom-monitoring platform from event generation through Kafka ingestion, Python processing, Spark window aggregation, SQLite storage, and a browser dashboard. Three classrooms produce time-stamped temperature, humidity, CO2, and occupancy readings. The stream includes documented missing values and injected spikes so cleaning and detection can be evaluated reproducibly."),
              P("A Python consumer computes rolling indicators, detects CO2 spikes, and predicts the next reading. Spark Structured Streaming calculates five-minute event-time summaries. The dashboard displays current readings, trends, alerts, predictions, and historical windows. The same seed and commands regenerate the experiments."),
              P(f"Offline evaluation on 2,160 synthetic events found spike precision {fmt(anomaly['precision'])}, recall {fmt(anomaly['recall'])}, and F1 {fmt(anomaly['f1'])}. The forecasting model achieved MAE {fmt(ml['model_mae'])} ppm, compared with {fmt(ml['baseline_mae'])} ppm for a last-value baseline. These figures measure this controlled generator, not real classrooms."),
              P("The live Kafka/Spark throughput and latency experiment is " + ("recorded on page 11." if bench and all(x.get("complete") for x in bench) else "not yet verified on this machine because Docker Desktop is not running. Page 11 marks those results as pending."))]
    page()
    story += [H("1 Introduction and problem statement"),
              P("University classrooms can become uncomfortable or poorly ventilated during a session. A useful monitoring system needs to receive readings continuously, display trends while a session is underway, and distinguish a sudden sensor excursion from a gradual rise in CO2. The purpose here is to demonstrate the complete data path and its measurable behavior on a reproducible synthetic source."),
              P("The unit of observation is one room reading per simulated minute. Three rooms make partitioning and independent per-room state visible. The replay rate can be increased for a short demonstration without changing the event timestamps. The dashboard explicitly calls those timestamps simulated event time; wall-clock time is recorded separately for latency measurement."),
              P("Success means that a generated event can be followed through the raw Kafka topic, Python validation and inference, processed Kafka topic, Spark event-time aggregation, SQLite persistence, and dashboard. The same project also needs defensible anomaly and forecasting metrics, performance measurements, and an explanation of limitations."),
              table([["Question", "Evidence"], ["Can the system process incrementally?", "Producer, consumer, stored events, and updating dashboard"],
                     ["Are alerts and predictions meaningful?", "Injected-label detection metrics and chronological forecast test"],
                     ["Does distributed processing work?", "Three Kafka partitions, three consumer workers, and Spark windows"]], [6*cm, 10*cm])]
    page()
    story += [H("2 Technology and method selection"),
              P("Python is used for the generator, validation, cleaning, rolling analytics, model training, inference, storage consumer, dashboard, and experiments. NumPy supplies deterministic random generation and rolling statistics. scikit-learn supplies a small gradient-boosting regression model. SQLite keeps a local reproducible record without a separate database server."),
              P("Kafka decouples producers from consumers and provides a partitioned event log. The room ID is the message key, so a room's events remain ordered within one partition. Three Python workers share one Kafka consumer group. Spark reads the processed topic and computes five-minute event-time windows with a two-minute watermark; the resulting summaries return to a Kafka topic before storage."),
              P("A built-in Python HTTP server serves the dashboard, avoiding an extra web framework. Docker Compose runs the Kafka broker and Spark runtime. Their exact versions are pinned in docker-compose.yml; Python dependencies are specified in requirements.txt and installed into the existing virtual environment on each machine."),
              table([["Layer", "Selection", "Reason"], ["Transport", "Kafka 3.9.1", "Partitioned durable log"],
                     ["Window processor", "Spark 3.5.7", "Event-time aggregation"], ["Storage", "SQLite", "Simple local persistence"],
                     ["Forecast", "Gradient boosting", "Small tabular regression model"]], [4*cm, 4*cm, 8*cm])]
    page()
    story += [H("3 System requirements and architecture"),
              P("The source must be continuous and reproducible, records must be validated before analytics, current and historical values must remain visible, and all relevant outputs must be persisted. The architecture below is one connected pipeline. Spark consumes the same processed events that drive the dashboard and writes summaries that the dashboard also reads."),
              table([["Stage", "Input", "Output"], ["Generator", "Fixed seed and simulated clock", "classroom.raw"],
                     ["Three Python workers", "classroom.raw", "classroom.processed"],
                     ["Spark", "classroom.processed", "classroom.windows"],
                     ["Storage consumer", "All three topics", "SQLite tables"],
                     ["Dashboard", "SQLite", "Current and historical browser view"]], [4*cm, 5*cm, 7*cm]),
              P("Architecture: Generator -> Kafka raw topic -> Python validation, cleaning, analytics, anomaly detection and ML inference -> Kafka processed topic -> Spark event-time windows -> Kafka windows topic -> SQLite -> browser dashboard. The storage consumer also subscribes to the raw and processed topics, preserving every stage for inspection."),
              P("A single broker and local Spark executor demonstrate partitioned and parallel processing but are not a fault-tolerant production cluster. A production deployment would add replicated brokers, durable external storage, monitoring, and managed model updates.")]
    story.append(Image(str(architecture), width=16*cm, height=4.65*cm))
    page()
    story += [H("4 Data source and acquisition"),
              P("core.py generates one reading for each of A101, B202, and C303 per simulated minute. The default stream contains 240 minutes per room, or 720 events. An evaluation stream uses 720 minutes per room. Seeds 42 and 43 make the demo and anomaly evaluation reproducible while keeping evaluation noise separate. Occupancy and daily phase affect slowly changing CO2, temperature, and humidity; random noise gives realistic local variation. Injected CO2 and temperature spikes and occasional missing temperature values exercise cleaning and detection."),
              table([["Field", "Type and meaning"], ["event_id", "Unique room/minute identifier"],
                     ["timestamp", "UTC simulated event time, ISO 8601"], ["room_id", "A101, B202, or C303"],
                     ["temperature_c", "Degrees Celsius; may be missing"], ["humidity_pct", "Relative humidity percentage"],
                     ["co2_ppm", "CO2 concentration in ppm"], ["occupancy", "Synthetic person count"],
                     ["injected_anomaly", "Evaluation label; ignored by detector"],
                     ["sent_at_epoch", "Wall-clock send time for latency"]], [5*cm, 11*cm]),
              P("The synthetic source is educational and does not represent actual classrooms or students. The injected label is kept out of features and detection logic, so the detector is judged against information it did not use. The event timestamp and wall-clock send time serve different purposes and are never treated as interchangeable.")]
    page()
    story += [H("5 Stream-processing pipeline"),
              P("Each Python worker consumes raw events as they arrive. Validation rejects missing keys, unknown rooms, invalid timestamps, nonfinite numbers, and implausible ranges. A missing temperature is replaced by the room's recent median or a documented initial default. The processed event keeps a list of cleaned fields, making the correction auditable."),
              P("The worker updates per-room ten-reading deques. It reports rolling CO2 mean and standard deviation, a sudden-spike anomaly decision, a separate high-CO2 operational alert, and a forecast. It publishes the processed record before committing the raw Kafka offset. The storage consumer writes raw, processed, and Spark summary records to SQLite before committing its offset."),
              P("Kafka and SQLite together provide replay and idempotent storage by event ID. A crash can still cause a processed event to be sent twice between publish and offset commit; SQLite replaces the same event ID. The per-worker rolling state starts fresh after a restart, so early post-restart rolling values have a warm-up period. This limitation is stated rather than hidden."),
              table([["Check", "Behavior"], ["Missing temperature", "Recent median; recorded in cleaned_fields"],
                     ["Invalid room or range", "Rejected and logged"], ["Repeated event ID", "SQLite upsert keeps one processed row"],
                     ["Consumer restart", "Kafka replays uncommitted records"]], [5*cm, 11*cm])]
    page()
    story += [H("6 Real-time analytics and visualization"),
              P("The dashboard refreshes every two seconds. It shows the latest temperature, humidity, CO2, and next-reading forecast for each classroom; a sixty-reading CO2 history; recent anomaly alerts; and Spark five-minute window summaries. Raw and processed record counts help demonstrate that the whole pipeline is advancing."),
              P("A ten-reading rolling window represents ten simulated minutes and is short enough to expose a sudden rise while smoothing noise. The Spark window is five simulated minutes, aligned to event time; it reports event count, average CO2, average temperature, and anomaly count per room. The two-minute watermark bounds late-arrival state and means a completed window may appear after later event timestamps arrive."),
              Image(str(chart), width=16*cm, height=5.3*cm),
              P("The figure is generated directly from the fixed-seed source. The browser graph uses the stored processed values, so the user can compare live current readings with the earlier history. The dashboard is served only on localhost for a classroom demonstration.")]
    page()
    story += [H("7 Time-series analysis and anomaly detection"),
              P("The CO2 stream has persistence from minute to minute, gradual occupancy-related changes, noise, and occasional injected jumps. A ten-reading per-room rolling baseline is therefore a simple choice for a local sudden-spike detector. After five prior readings, a new CO2 value is marked anomalous when it exceeds the prior mean by more than the larger of 300 ppm or four prior standard deviations. CO2 above 1,300 ppm is separately marked as a high-level operational alert."),
              P(f"On the 2,160-event evaluation stream there were {anomaly['labeled_spikes']} injected spikes. The detector produced {anomaly['tp']} true positives, {anomaly['fp']} false positives, {anomaly['fn']} false negatives, and {anomaly['tn']} true negatives. Precision was {fmt(anomaly['precision'])}, recall {fmt(anomaly['recall'])}, and F1 {fmt(anomaly['f1'])}."),
              table([["Actual / predicted", "Spike", "Normal"], ["Injected spike", anomaly["tp"], anomaly["fn"]],
                     ["Normal", anomaly["fp"], anomaly["tn"]]], [6*cm, 5*cm, 5*cm]),
              P("This strong score reflects a controlled synthetic spike pattern. It does not establish performance on real sensors, drift, long outages, or subtle faults. The high-CO2 threshold answers a different operational question and is not included in spike precision or recall.")]
    page()
    story += [H("8 Machine-learning component"),
              P("The model predicts next-minute CO2 from the current CO2, temperature, humidity, occupancy, and sine/cosine time-of-day features. Training pairs with injected spikes or missing current temperature are excluded. Each room contributes data, but the split is chronological: the first 70% of simulated time is training and the last 30% is test. This avoids random-split leakage across neighboring readings."),
              P(f"The saved model used {ml['train_pairs']} training pairs and {ml['test_pairs']} test pairs. On the future test period it achieved MAE {fmt(ml['model_mae'])} ppm and RMSE {fmt(ml['model_rmse'])} ppm. A persistence baseline that predicts the current value for the next minute achieved MAE {fmt(ml['baseline_mae'])} ppm and RMSE {fmt(ml['baseline_rmse'])} ppm."),
              table([["Method", "MAE (ppm)", "RMSE (ppm)"], ["Gradient boosting", fmt(ml["model_mae"]), fmt(ml["model_rmse"])],
                     ["Last-value baseline", fmt(ml["baseline_mae"]), fmt(ml["baseline_rmse"])]], [7*cm, 4.5*cm, 4.5*cm]),
              P("The improvement is small, which is expected for a smooth one-step series with a strong persistence baseline. Real-time inference loads the saved model once in each Python worker and predicts each cleaned event before publishing it. The dashboard presents the forecast alongside the latest observed CO2 value.")]
    page()
    story += [H("9 Distributed stream-processing design"),
              P("The raw and processed topics each have three partitions. The producer keys every record by room ID, preserving per-room order within a partition. Three Python consumers share the classroom-processors group, allowing Kafka to assign work across them. Spark uses local[2] in its container to execute its streaming transformations in parallel and reads all processed partitions."),
              P("Spark parses the processed JSON schema, converts the event timestamp to a timestamp with UTC session timezone, applies a two-minute watermark, and groups by five-minute event-time window plus room. It writes completed summaries to classroom.windows. A separate storage consumer subscribes to all three topics and persists them. This creates a producer-consumer architecture with multiple stages and actual partitioned processing."),
              P("This is an instructional single-host deployment. Kafka replication factor one and Spark local mode are the explicit limits. The design can scale by adding broker replicas, more topic partitions and consumers, and a Spark cluster, but those changes were not claimed as measured here."),
              table([["Topic", "Partitions", "Key", "Consumer"], ["classroom.raw", "3", "room_id", "Python workers and storage"],
                     ["classroom.processed", "3", "room_id", "Spark and storage"],
                     ["classroom.windows", "3", "room_id", "Storage"]], [5*cm, 3*cm, 3*cm, 5*cm])]
    page()
    story += [H("10 Experimental setup and results"),
              P("The experiment is reproducible with the existing virtual environment on each machine, the pinned Docker Compose services, and a fixed generator seed. The offline evaluation commands are `python app.py train` and `python app.py evaluate`. The live demonstration starts Kafka, creates three topics, launches three Python processors plus storage and dashboard processes, starts Spark, and replays 720 readings."),
              P("Anomaly and forecast metrics are generated by those commands and stored as JSON in artifacts. The plotted source data comes from the same generator. The browser and SQLite counts are execution evidence for the live path; screenshots should be taken during the demonstration after the Docker services are running."),
              table([["Experiment", "Result"], ["Anomaly precision / recall / F1", f"{fmt(anomaly['precision'])} / {fmt(anomaly['recall'])} / {fmt(anomaly['f1'])}"],
                     ["Forecast model MAE / RMSE", f"{fmt(ml['model_mae'])} / {fmt(ml['model_rmse'])} ppm"],
                     ["Forecast baseline MAE / RMSE", f"{fmt(ml['baseline_mae'])} / {fmt(ml['baseline_rmse'])} ppm"],
                     ["Cumulative live storage evidence", f"{counts['raw']} raw, {counts['processed']} processed, {counts['windows']} windows" if counts else "Pending live run"]], [8*cm, 8*cm]),
              P("The detector sees processed values but never injected_anomaly. The model does not train on the future test period. These two separations keep the reported offline results interpretable.")]
    page()
    perf_rows = [["Target /s", "Received", "Observed /s", "Mean latency ms", "System CPU %"]]
    if bench:
        perf_rows += [[str(x["target_events_per_second"]), str(x["received"]),
                       fmt(x["observed_events_per_second"]), fmt(x["mean_end_to_end_latency_ms"]),
                       fmt(x["system_cpu_percent"])] for x in bench]
    else:
        perf_rows += [[str(rate), "pending", "pending", "pending", "pending"] for rate in (10, 100, 500)]
    if bench and all(x.get("complete") for x in bench):
        perf_analysis = (f"Observed throughput rose from {fmt(bench[0]['observed_events_per_second'])} to "
                         f"{fmt(bench[-1]['observed_events_per_second'])} events/s as offered load rose, "
                         f"but it remained below the 500 events/s target. At that load the mean latency was "
                         f"{fmt(bench[-1]['mean_end_to_end_latency_ms'])} ms and sampled system CPU was "
                         f"{fmt(bench[-1]['system_cpu_percent'])}%. CPU saturation is consistent with the "
                         "shortfall, but this system-wide sample cannot isolate Python, Kafka, Spark, or SQLite. "
                         "The first optimization to test is batching SQLite commits, using the same three loads.")
    else:
        perf_analysis = ("The likely single-host bottlenecks are Kafka and SQLite writes and Spark "
                         "micro-batch overhead. This is a design hypothesis until the live benchmark is complete. "
                         "The first optimization to test is batching SQLite commits, comparing the same three "
                         "loads before and after the change.")
    story += [H("11 Performance and scalability analysis"),
              P(f"Benchmark host: {bench[0].get('host', 'unspecified development machine') if bench else 'not yet measured'}. Throughput and latency depend on the machine; rerun this experiment on the presentation laptop for laptop-specific values."),
              P("The benchmark publishes 300 events at each target rate of 10, 100, and 500 events per second through the same Kafka and Python path used by the dashboard. It waits for each event ID to appear in SQLite. Observed throughput divides stored count by wall-clock time from the start of sending to final storage. End-to-end latency uses the producer's wall-clock send timestamp and the storage consumer's insert time."),
              P("System CPU is sampled during each run. It includes other machine activity and is therefore a coarse context measure, not isolated Spark or broker CPU. Spark's window results arrive after watermark advancement, so the benchmark focuses on the event path to persisted processed records."),
              table(perf_rows, [3*cm, 3*cm, 3.5*cm, 3.5*cm, 3*cm]),
              P(perf_analysis)]
    page()
    story += [H("12 Limitations and future improvements"),
              P("All sensor data is simulated. Its noise and injected spikes are simpler than real sensor drift, outages, calibration differences, and seasonal patterns. Perfect or near-perfect spike metrics on this generator must not be generalized to real classrooms. The forecasting improvement over the last-value baseline is modest, and a real deployment would need longer data collection and periodic retraining."),
              P("The Kafka broker has one replica and Spark runs in one container on one computer. Three partitions and consumers show parallel stages but do not prove fault tolerance or cluster-scale speed. SQLite is appropriate for a demonstration, while a larger deployment would need a shared database and stronger operational monitoring. Per-room rolling state is in worker memory and warms up after a restart."),
              P("Next steps are to connect real sensors with consent and calibration, add out-of-order and duplicate-event tests, persist or reconstruct rolling state across restarts, compare more robust anomaly methods, and measure the same benchmark after batching writes and increasing partitions. Those are proposed improvements, not claims about the current implementation."),
              H("13 Conclusion"),
              P("The project joins acquisition, incremental processing, rolling analytics, visualization, time-series analysis, anomaly detection, machine-learning inference, partitioned Kafka processing, Spark event-time windows, persistence, and reproducible evaluation in one system. Offline metrics are recorded above; live performance conclusions require the Docker-backed benchmark.")]
    page()
    story += [H("14 References and reproducibility"),
              P("Primary technical references used for implementation:"),
              small("Apache Kafka 3.9 Docker documentation: https://kafka.apache.org/39/getting-started/docker/"),
              small("Apache Kafka single-node Compose example: https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml"),
              small("Apache Spark 3.5.7 Structured Streaming and Kafka integration: https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html"),
              small("scikit-learn HistGradientBoostingRegressor documentation: https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html"),
              P("Reproduction on the Apple Silicon Mac: clone inside `projects-for-uni` beside the existing Python 3.14.5 `venv`, start Docker Desktop, run `bash setup_laptop.sh`, then `bash run_demo.sh`; open http://127.0.0.1:8000. Run the benchmark and regenerate the PDFs with `../venv/bin/python`, then stop with `bash stop_demo.sh`. Windows launch scripts are also provided. The README gives exact commands for both systems."),
              P("The synthetic generator, validation rules, anomaly detector, model training, Spark job, dashboard, and benchmarking code are all included in this submission. The source brief is `Final_Project_Real_Time_Data_Analysis.docx.pdf` in the project root."),
              P("Submission status: " + ("live benchmark recorded." if bench and all(x.get("complete") for x in bench) else "offline evaluation complete; live Kafka/Spark validation and benchmark pending Docker Desktop setup."))]
    doc = SimpleDocTemplate(str(OUT / "Final_Technical_Report.pdf"), pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm, topMargin=2.2*cm, bottomMargin=2*cm)
    doc.build(story, onFirstPage=report_page, onLaterPages=report_page)


def build_slides(ml, anomaly, bench, chart, architecture, counts):
    path = OUT / "Presentation.pdf"
    c = canvas.Canvas(str(path), pagesize=landscape(A4))
    W, H = landscape(A4)
    def base(title, num):
        c.setFillColor(colors.HexColor("#f4f7fb")); c.rect(0,0,W,H,fill=1,stroke=0)
        c.setFillColor(BLUE); c.setFont("Helvetica-Bold", 24); c.drawString(48,H-60,title)
        c.setStrokeColor(TEAL); c.setLineWidth(3); c.line(48,H-72,W-48,H-72)
        c.setFillColor(colors.HexColor("#5d7082")); c.setFont("Helvetica", 10)
        c.drawString(48,25,"Yerlan Utepbergenov | Real-Time Data Analysis")
        c.drawRightString(W-48,25,str(num))
    def lines(items, y=H-115, size=17):
        c.setFillColor(BLUE); c.setFont("Helvetica",size)
        for item in items:
            c.drawString(60,y,"• " + item); y -= 35
    def end(): c.showPage()
    base("Classroom Sensor Analytics",1); lines(["An end-to-end Kafka, Python, Spark and SQLite platform", "Synthetic classroom readings, live dashboard and reproducible evaluation", "Final project | 17 November 2026"]); end()
    base("Problem and goal",2); lines(["Receive time-stamped readings continuously", "Detect sudden CO2 spikes and show room trends", "Predict next-minute CO2", "Persist raw, processed and aggregated results"]); end()
    base("One connected architecture",3); c.drawImage(str(architecture),45,130,width=W-90,height=270,preserveAspectRatio=True,anchor='c'); c.setFont("Helvetica",13); c.drawString(55,100,"Kafka topics and SQLite connect every stage of the project."); end()
    base("Synthetic data and schema",4); lines(["3 rooms, 1 reading per simulated minute", "Temperature, humidity, CO2 and occupancy", "Fixed seed; known injected spikes and missing values", "Event time is distinct from wall-clock send time"]); end()
    base("Incremental processing",5); lines(["Validate schema and ranges; clean missing temperature", "10-reading rolling CO2 mean and standard deviation", "Separate sudden-spike anomaly and high-CO2 alert", "Predict each cleaned event before publishing"]); end()
    base("Time-series and dashboard",6); c.drawImage(str(chart),50,100,width=W-100,height=250,preserveAspectRatio=True,anchor='c'); c.setFont("Helvetica",12); c.drawString(55,80,"Live browser view refreshes every 2 seconds; Spark windows use event time."); end()
    base("Anomaly evaluation",7); lines([f"Injected spikes: {anomaly['labeled_spikes']} of {anomaly['events']} events", f"Precision: {fmt(anomaly['precision'])}    Recall: {fmt(anomaly['recall'])}    F1: {fmt(anomaly['f1'])}", f"TP {anomaly['tp']}   FP {anomaly['fp']}   FN {anomaly['fn']}   TN {anomaly['tn']}", "Controlled synthetic result; real sensor behavior may differ"]); end()
    base("Forecast evaluation",8); lines([f"Chronological split: {ml['train_pairs']} train, {ml['test_pairs']} test pairs",f"Gradient boosting MAE: {fmt(ml['model_mae'])} ppm",f"Last-value baseline MAE: {fmt(ml['baseline_mae'])} ppm", "One-minute-ahead prediction is served by Python workers"]); end()
    base("Performance experiment",9)
    if bench:
        lines([f"{x['target_events_per_second']}/s target -> {x['observed_events_per_second']} stored/s, {fmt(x['mean_end_to_end_latency_ms'])} ms" for x in bench] + ["Measured from producer send to SQLite storage", f"Host: {bench[0].get('host', 'unspecified')}"])
    else:
        lines(["300 events each at 10, 100 and 500 events/s", "Throughput and latency measured end to end", "Live measurements pending Docker Desktop setup"])
    end()
    base("Live demonstration",10); lines(["On the Mac: bash run_demo.sh; open localhost:8000", "Watch readings, alerts, forecasts and Spark windows", f"Stored: {counts['raw']} raw, {counts['processed']} processed, {counts['windows']} windows" if counts else "Inspect SQLite and Kafka topic counts", "Run benchmark, then regenerate the report and slides"]); end()
    base("Limitations and conclusion",11); lines(["Synthetic data; single broker and local Spark", "In-memory rolling state warms up after restart", "Next: real sensors, state recovery and scaled benchmarks", "One reproducible pipeline covers all 10 required components"]); end()
    c.save()


def main():
    OUT.mkdir(exist_ok=True)
    ml = read_json("ml_metrics.json")
    anomaly = read_json("anomaly_metrics.json")
    if not ml or not anomaly:
        raise SystemExit("Run python app.py train and python app.py evaluate first")
    bench = read_json("benchmark.json")
    counts = live_counts()
    chart = make_chart()
    architecture = make_architecture()
    build_report(ml, anomaly, bench, chart, architecture, counts)
    build_slides(ml, anomaly, bench, chart, architecture, counts)
    print("Created", OUT / "Final_Technical_Report.pdf", "and", OUT / "Presentation.pdf")


if __name__ == "__main__":
    main()
