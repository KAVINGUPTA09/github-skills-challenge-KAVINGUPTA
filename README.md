# AIOps Simulation - Monitoring & Event Processing

## 1. AIOps Scenario
In this environment, an application service (`payment-service`) is monitored through operational metrics (CPU, memory, response time) and structured logs. The objective is to automate anomaly identification and alert propagation without requiring manual log inspection.

## 2. Operational Data Description
The dataset in `data/service_data.json` contains:
- **Metrics**: `response_time_ms`, `cpu_percent`, `memory_percent`
- **Log Data**: `log_level` (INFO, ERROR, WARN), `message`
- **Context/Timestamps**: ISO 8601 formatted timestamps (`YYYY-MM-DDTHH:MM:SS`) tracking service metrics over time.

## 3. Observations from Logs & Metrics
- **Normal Observations**: Low latency (<200ms), standard CPU/memory utilisation (<60%), and `INFO` log levels.
- **Unusual Observations**: Spike in latency (>500ms) paired with `ERROR` level logs and elevated resource usage (>70%), indicating service degradation or timeout events.

## 4. Anomaly-Detection Findings
The rule-based detector flags records where metrics breach specified operational thresholds or log levels indicate errors (`ERROR`). It extracts the affected service, timestamp, reason, and severity.

## 5. Event-Processing Flow
`Operational Data` → `AnomalyDetector` → `Event Generation` → `EventProducer.publish()` → `EventTopic ("anomaly-events")` → `EventConsumer.consume()` → `Downstream AIOps Reporting`

## 6. Execution Results
When executed via `python src/aiops_pipeline.py`, all 10 service events are processed, 2 anomalies are flagged, routed through the shared topic, and consumed for final output verification.

## 7. Issues Identified & Corrected
- **Issue #1 (Topic Decoupling / Mismatch)**: `EventProducer` published messages to `"service-events"`, while `EventConsumer` listened to `"anomaly-events"`, preventing downstream detection. Fixed by routing both through the shared topic `EventTopic("anomaly-events")`.
- **Issue #2 (Test Coverage)**: Tests omitted invocations of `run_pipeline` and `load_data`. Added comprehensive unit tests in `tests/test_aiops_pipeline.py` achieving 89% overall coverage.

## 8. Limitations & Potential Improvements
- **Threshold Staticity**: Current anomaly detection relies on fixed hardcoded thresholds. It can be improved by introducing dynamic, statistical baselining (e.g., moving averages or Z-score models) to handle normal traffic spikes without false positives.

