import json
import sys
from pathlib import Path

# Add src to sys.path so sibling imports like "from event_topic import ..." always resolve
src_dir = Path(__file__).resolve().parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from anomaly_detector import AnomalyDetector
from event_consumer import EventConsumer
from event_producer import EventProducer
from event_topic import EventTopic


def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def run_pipeline(file_path):
    data = load_data(file_path)

    # Issue #2 & #3 Fix: Producer and consumer must use the exact same topic instance
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)
    detector = AnomalyDetector()

    detected_events = []

    for record in data:
        event = detector.detect(record)
        if event:
            producer.publish(event)
            detected_events.append(event)

    consumed_events = consumer.consume()

    return {
        "records_processed": len(data),
        "anomalies_detected": detected_events,
        "events_consumed": consumed_events,
    }


def main():
    result = run_pipeline("data/service_data.json")

    print("=" * 50)
    print("AIOps Pipeline Result")
    print("=" * 50)
    print(f"Records processed: {result['records_processed']}")
    print(f"Anomalies detected: {len(result['anomalies_detected'])}")
    print(f"Events consumed: {len(result['events_consumed'])}")

    print("\nDetected Events:")
    for event in result["events_consumed"]:
        print(f"\nService: {event.get('service', 'unknown')}")
        print(f"Timestamp: {event.get('timestamp', 'unknown')}")
        print(f"Type: {event.get('type', 'unknown')}")
        if "reasons" in event:
            print(f"Reasons: {', '.join(event['reasons'])}")

    return result


if __name__ == "__main__":
    main()
