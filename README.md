# Real-Time Classroom Sensor Analytics

Final project for Real-Time Data Analysis, by Yerlan Utepbergenov. This is a **synthetic educational system**. It does not contain data from real classrooms.

## What it does

Three simulated classrooms emit one timestamped reading per simulated minute. The replay rate is adjustable. Each live run gets a unique ID and reserves the next simulated time range, so repeated demos add new records and Spark windows. The pipeline validates and cleans each event, calculates ten-reading rolling statistics, detects sudden CO2 spikes, predicts the next CO2 value, computes five-minute Spark windows, stores all stages in SQLite, and updates a browser dashboard every two seconds.

```text
Generator -> Kafka classroom.raw (3 partitions, keyed by room)
          -> 3 Python workers: validation, cleaning, rolling statistics, anomaly and ML
          -> Kafka classroom.processed -> Spark event-time windows
          -> Kafka classroom.windows -> SQLite -> browser dashboard
          \---- raw and processed topics also feed the SQLite storage consumer
```

The architecture diagram is also in `artifacts/architecture.png` and the technical report.

## Reproduce on an Apple Silicon Mac

The presentation laptop needs macOS, Docker Desktop running with Linux containers, and a **Python 3.12 virtual environment in the folder above this repository** at `../.venv`. The laptop already has Docker and a parent virtual environment, so use that one. No virtual environment is created by these scripts. If its path differs, set `PYTHON_BIN` to the full path of its Python executable. Internet access is needed once to pull the pinned Kafka and Spark ARM64 images and Spark's Kafka connector package. Use a terminal in the cloned repository:

```bash
bash setup_laptop.sh
bash run_demo.sh
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000). The first image pull and connector download may take several minutes. The script trains the model, evaluates anomaly detection, starts Kafka, creates the three topics, launches three Python processors plus storage and dashboard, starts Spark, and replays 720 events at 30 events/s. Wait for the dashboard to show processed readings **and** Spark windows before presenting.

To send another stream, measure performance, and rebuild the PDFs on the laptop:

```bash
../.venv/bin/python app.py produce --minutes 240 --rate 30
../.venv/bin/python app.py benchmark
../.venv/bin/python make_documents.py
bash stop_demo.sh
```

`PYTHON_BIN` can replace `../.venv/bin/python` in those commands if the laptop's environment has another location. Python virtual environments cannot be copied reliably between Windows and macOS; install the pinned dependencies into the laptop's existing environment with `setup_laptop.sh`.

## Run on Windows

On the current Windows PC, use the existing `..\.venv\Scripts\python.exe`. Do not create another virtual environment. Start Docker Desktop, then run from PowerShell:

```powershell
& '..\.venv\Scripts\python.exe' -m pip install -r requirements.txt
.\run_demo.ps1
& '..\.venv\Scripts\python.exe' app.py benchmark
& '..\.venv\Scripts\python.exe' make_documents.py
.\stop_demo.ps1
```

The dashboard shows simulated event timestamps in UTC. The benchmark sends 300 events at each target rate of 10, 100, and 500 events/s and saves measured results in `artifacts/benchmark.json`. Regenerating documents inserts those results into the PDF report and slides. Measurements can differ on the laptop; the committed PDFs contain the results measured on the Windows PC.

`docker compose down` preserves Kafka's named volume; the SQLite file and Spark checkpoint also persist. For a clean experiment, first stop the demo, then remove `data/classroom.sqlite`, `data/spark-checkpoint-v2`, and `artifacts/benchmark.json`. That reset deletes prior local experiment output. To reset Kafka too, run `docker compose down -v` after stopping the demo; this deletes the Kafka volume. Reset Kafka and the Spark checkpoint together so offsets remain consistent.

## Event schema and methods

| Field | Meaning |
| --- | --- |
| `event_id` | Unique run/room/minute identifier |
| `timestamp` | Simulated UTC event time, ISO 8601 |
| `room_id` | `A101`, `B202`, or `C303`; Kafka partition key |
| `temperature_c` | Temperature in Celsius; may be missing and imputed |
| `humidity_pct` | Relative humidity percentage |
| `co2_ppm` | CO2 concentration in ppm |
| `occupancy` | Synthetic person count |
| `injected_anomaly` | Generator's evaluation label; detector ignores it |
| `sent_at_epoch` | Wall-clock send time used only for latency |

The demo and model-training generator use seed 42. Anomaly evaluation uses a separate seed 43. Training uses the earlier 70% of simulated time and tests on the later 30%. It predicts next-minute CO2 with current CO2, temperature, humidity, occupancy, and cyclic time-of-day features. A last-value baseline makes the model's gain interpretable. The anomaly detector compares each CO2 value with the preceding ten per-room readings after five warm-up readings, using `max(300 ppm, 4 × prior standard deviation)` as its sudden-spike threshold. A separate `high_co2_alert` records levels above 1,300 ppm. Spike precision, recall, F1, and confusion counts are evaluated against injected labels; real sensor accuracy is unknown.

Spark uses five-minute tumbling event-time windows, a two-minute watermark, and UTC. A window appears after later event timestamps advance the watermark. The benchmark measures the path from producer send to SQLite processed storage; it does not treat Spark window completion as per-event latency. Its CPU number is a system sample, not isolated broker CPU.

## Files and outputs

| File | Purpose |
| --- | --- |
| `core.py` | Reproducible source, validation, rolling processing, model and anomaly evaluation |
| `app.py` | Kafka producer/consumers, SQLite sink, local dashboard, benchmark |
| `spark_job.py` | Spark Structured Streaming window job |
| `docker-compose.yml` | Kafka and Spark services |
| `setup_laptop.sh`, `run_demo.sh`, `stop_demo.sh` | Apple Silicon macOS setup, demo, and shutdown |
| `run_demo.ps1`, `stop_demo.ps1` | Windows demo and shutdown |
| `make_documents.py` | Builds the report, slides, plots and architecture diagram from saved metrics |
| `artifacts/Final_Technical_Report.pdf` | 14-page technical report |
| `artifacts/Presentation.pdf` | 11-slide presentation for a 10–15 minute talk |
| `data/classroom.sqlite` | `raw`, `processed`, and `windows` tables |

Model metrics, anomaly metrics, and benchmark results are JSON files in `artifacts/`. Logs for background Python workers are in `data/`; Spark logs are available with `docker compose logs spark`. The report and slides can be regenerated after a fresh experiment.

## Requirement checklist

| PDF requirement | Implemented evidence |
| --- | --- |
| 1. Data acquisition | Fixed-seed `generate()` in `core.py`; documented schema above |
| 2. Stream processing | Kafka consumers in `app.py`; `validate()` and `Processor.process()` |
| 3. Real-time analytics | Ten-reading rolling mean and standard deviation; Spark five-minute windows |
| 4. Visualization | Refreshing browser dashboard at `localhost:8000` |
| 5. Time-series analysis | Per-room trends, justified ten-reading and five-minute windows in report |
| 6. Anomaly detection | Sudden-spike detector and precision, recall, F1 in `anomaly_metrics.json` |
| 7. Machine learning | Next-minute CO2 model and chronological MAE/RMSE evaluation in `ml_metrics.json` |
| 8. Distributed processing | Three Kafka partitions, three Python workers, and Spark streaming stage |
| 9. Performance evaluation | Three measured rates, throughput, latency, CPU in `benchmark.json` |
| 10. Persistence | SQLite `raw`, `processed`, and `windows` tables plus restartable Kafka and Spark state |

## Live demo sequence

1. Start `bash run_demo.sh` on the Mac; open the dashboard and point out current readings and CO2 trend.
2. Show alerts, next-minute predictions, and Spark window summaries as data arrives.
3. Explain the three Kafka partitions and three Python consumers. Open `artifacts/architecture.png` if useful.
4. Show `artifacts/ml_metrics.json` and `artifacts/anomaly_metrics.json`; explain the synthetic-label limitation.
5. Run the benchmark and show `artifacts/benchmark.json`, then regenerate the PDFs.

## References

- [Apache Kafka Docker documentation](https://kafka.apache.org/39/getting-started/docker/)
- [Apache Kafka single-node Compose example](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)
- [Spark 3.5.7 Kafka integration guide](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
- [scikit-learn HistGradientBoostingRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html)
