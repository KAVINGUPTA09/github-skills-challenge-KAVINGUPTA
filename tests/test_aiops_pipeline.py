import json
import tempfile
import sys
from pathlib import Path

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from anomaly_detector import AnomalyDetector
from aiops_pipeline import run_pipeline, load_data, main
from event_consumer import EventConsumer
from event_producer import EventProducer
from event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully",
    }
    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout",
    }
    event = detector.detect(record)
    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    event = {
        "type": "ANOMALY",
        "service": "payment-service",
    }
    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)
    event = {
        "type": "ANOMALY",
        "service": "payment-service",
    }
    producer.publish(event)
    messages = consumer.consume()
    assert len(messages) == 1


def test_load_data():
    sample = [{"test": "value"}]
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        json.dump(sample, f)
        temp_path = f.name

    loaded = load_data(temp_path)
    assert loaded == sample


def test_run_pipeline_with_data():
    sample_records = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "Normal request",
        },
        {
            "timestamp": "2026-09-20T10:05:00",
            "service": "payment-service",
            "response_time_ms": 610,
            "cpu_percent": 75,
            "memory_percent": 70,
            "log_level": "ERROR",
            "message": "Timeout error",
        },
    ]

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        json.dump(sample_records, f)
        temp_path = f.name

    result = run_pipeline(temp_path)
    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert len(result["events_consumed"]) == 1


def test_main():
    result = main()
    assert "records_processed" in result
    assert result["records_processed"] > 0
