"""Deterministic classroom data, cleaning, analytics, and evaluation."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import math

import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, precision_recall_fscore_support
import joblib

ROOMS = ("A101", "B202", "C303")
START = datetime(2026, 11, 17, 8, 0, tzinfo=timezone.utc)
FEATURES = ("co2_ppm", "temperature_c", "humidity_pct", "occupancy", "minute_sin", "minute_cos")


def generate(minutes: int = 240, seed: int = 42, start: datetime = START):
    """One event per classroom per simulated minute, in event-time order."""
    rng = np.random.default_rng(seed)
    states = {room: 480.0 + i * 35 for i, room in enumerate(ROOMS)}
    for minute in range(minutes):
        t = start + timedelta(minutes=minute)
        for i, room in enumerate(ROOMS):
            occupancy = max(0, round(17 + 9 * math.sin(minute / 24 + i) + rng.normal(0, 2)))
            states[room] = max(410, 0.93 * states[room] + 0.07 * (430 + occupancy * 34) + rng.normal(0, 13))
            co2 = states[room]
            temp = 21.5 + 1.8 * math.sin(minute / 45 + i / 2) + occupancy * .045 + rng.normal(0, .2)
            humidity = 43 + 6 * math.sin(minute / 35 + i) + rng.normal(0, 1.4)
            injected = minute > 15 and (minute + 19 * i) % 83 == 0
            if injected:
                co2 += 650
                temp += 4.2
            missing = minute > 15 and (minute + 11 * i) % 97 == 0
            yield {
                "event_id": f"{room}-{minute:04d}", "timestamp": t.isoformat().replace("+00:00", "Z"),
                "room_id": room, "temperature_c": None if missing else round(temp, 2),
                "humidity_pct": round(float(np.clip(humidity, 15, 85)), 2),
                "co2_ppm": round(co2, 1), "occupancy": occupancy,
                "injected_anomaly": bool(injected),
            }


def validate(event: dict) -> tuple[bool, str]:
    if not isinstance(event, dict):
        return False, "not an object"
    required = ("event_id", "timestamp", "room_id", "temperature_c", "humidity_pct", "co2_ppm", "occupancy")
    if any(k not in event for k in required):
        return False, "missing field"
    if event["room_id"] not in ROOMS:
        return False, "unknown room"
    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        numeric = ("temperature_c", "humidity_pct", "co2_ppm")
        if any(event[k] is not None and not math.isfinite(float(event[k])) for k in numeric):
            return False, "nonfinite value"
        if not 0 <= float(event["humidity_pct"]) <= 100 or not 300 <= float(event["co2_ppm"]) <= 5000:
            return False, "out of range"
        if not isinstance(event["occupancy"], int) or not 0 <= event["occupancy"] <= 100:
            return False, "invalid occupancy"
        if event["temperature_c"] is not None and not -10 <= float(event["temperature_c"]) <= 60:
            return False, "invalid temperature"
    except (TypeError, ValueError, AttributeError):
        return False, "invalid type"
    return True, "ok"


def features(event: dict) -> list[float]:
    timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    phase = 2 * math.pi * (timestamp.hour * 60 + timestamp.minute) / 1440
    return [float(event["co2_ppm"]), float(event["temperature_c"]),
            float(event["humidity_pct"]), float(event["occupancy"]),
            math.sin(phase), math.cos(phase)]


class Processor:
    def __init__(self, model=None):
        self.model = model
        self.co2 = defaultdict(lambda: deque(maxlen=10))
        self.temp = defaultdict(lambda: deque(maxlen=10))
        self.humidity = defaultdict(lambda: deque(maxlen=10))

    def process(self, raw: dict) -> dict:
        ok, reason = validate(raw)
        if not ok:
            raise ValueError(reason)
        room = raw["room_id"]
        e = dict(raw)
        cleaned = []
        for key, history, default in (
            ("temperature_c", self.temp[room], 22.0),
            ("humidity_pct", self.humidity[room], 45.0),
            ("co2_ppm", self.co2[room], 500.0),
        ):
            if e[key] is None:
                e[key] = float(np.median(history)) if history else default
                cleaned.append(key)
            e[key] = float(e[key])
        prior = list(self.co2[room])
        mean = float(np.mean(prior)) if prior else e["co2_ppm"]
        std = max(float(np.std(prior)), 35.0) if prior else 35.0
        deviation = e["co2_ppm"] - mean
        e["anomaly"] = bool(len(prior) >= 5 and deviation > max(4 * std, 300))
        e["high_co2_alert"] = bool(e["co2_ppm"] > 1300)
        e["rolling_co2_mean"] = round(float(np.mean((prior + [e["co2_ppm"]])[-10:])), 1)
        e["rolling_co2_std"] = round(float(np.std((prior + [e["co2_ppm"]])[-10:])), 1)
        e["cleaned_fields"] = cleaned
        e["prediction_co2"] = round(float(self.model.predict([features(e)])[0]), 1) if self.model else None
        e["processed_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.co2[room].append(e["co2_ppm"])
        self.temp[room].append(e["temperature_c"])
        self.humidity[room].append(e["humidity_pct"])
        return e


def training_pairs(events: list[dict]):
    by_room = defaultdict(list)
    for e in events:
        by_room[e["room_id"]].append(e)
    pairs = []
    for room in ROOMS:
        rows = by_room[room]
        for a, b in zip(rows, rows[1:]):
            if a["injected_anomaly"] or b["injected_anomaly"]:
                continue
            if a["temperature_c"] is None:
                continue
            pairs.append((a, b["co2_ppm"]))
    return pairs


def train_and_evaluate(directory: Path, minutes: int = 720) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    pairs = training_pairs(list(generate(minutes=minutes)))
    # All rooms use the same chronological cutoff, preventing future data leakage.
    cutoff = START + timedelta(minutes=int(minutes * .7))
    train = [(a, y) for a, y in pairs if datetime.fromisoformat(a["timestamp"].replace("Z", "+00:00")) < cutoff]
    test = [(a, y) for a, y in pairs if datetime.fromisoformat(a["timestamp"].replace("Z", "+00:00")) >= cutoff]
    model = HistGradientBoostingRegressor(max_iter=80, max_leaf_nodes=12, l2_regularization=1, random_state=42)
    model.fit([features(a) for a, _ in train], [y for _, y in train])
    true = np.array([y for _, y in test])
    pred = model.predict([features(a) for a, _ in test])
    baseline = np.array([a["co2_ppm"] for a, _ in test])
    metrics = {
        "train_pairs": len(train), "test_pairs": len(test),
        "model_mae": float(mean_absolute_error(true, pred)),
        "model_rmse": float(np.sqrt(mean_squared_error(true, pred))),
        "baseline_mae": float(mean_absolute_error(true, baseline)),
        "baseline_rmse": float(np.sqrt(mean_squared_error(true, baseline))),
        "chronological_split": "first 70% train, last 30% test",
    }
    joblib.dump(model, directory / "co2_model.joblib")
    (directory / "ml_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def evaluate_anomalies(directory: Path, minutes: int = 720) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    processor = Processor()
    true, pred = [], []
    for event in generate(minutes=minutes, seed=43):
        result = processor.process(event)
        true.append(event["injected_anomaly"])
        pred.append(result["anomaly"])
    p, r, f, _ = precision_recall_fscore_support(true, pred, average="binary", zero_division=0)
    matrix = {"tp": sum(a and b for a, b in zip(true, pred)),
              "fp": sum(not a and b for a, b in zip(true, pred)),
              "fn": sum(a and not b for a, b in zip(true, pred)),
              "tn": sum(not a and not b for a, b in zip(true, pred))}
    metrics = {"precision": float(p), "recall": float(r), "f1": float(f), **matrix,
               "evaluation_seed": 43,
               "events": len(true), "labeled_spikes": sum(true),
               "note": "Labels describe injected spikes; high CO2 is tracked separately as an operational alert."}
    (directory / "anomaly_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics
