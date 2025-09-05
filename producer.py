from kafka import KafkaProducer
import json
import time

KAFKA_TOPIC = "flight-delays"
KAFKA_SERVER = "localhost:9092"

print("Starting the live data producer...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate a complete, rich data record for a single flight
final_message = {
    "flight_iata": "AI805",
    "airline_name": "Air India",
    "departure_airport": "Delhi (VIDP)",
    "arrival_airport": "Mumbai (VABB)",
    "status": "en-route",
    "latitude": 28.5562,
    "longitude": 77.1000,
    "altitude": 9144.0,
    "speed_kph": 850,
    "weather_temp_c": 34.5,
    "weather_wind_kph": 12.0,
    "dep_scheduled": "2025-09-01T20:00:00Z",
    "dep_estimated": "2025-09-01T20:15:00Z",
    "arr_scheduled": "2025-09-01T22:00:00Z",
    "arr_estimated": "2025-09-01T22:25:00Z",
    "disaster_alert": "Cyclone Warning: High winds reported over the Arabian Sea."
}

print(f"\nSending the following complete flight data to Kafka:\n{json.dumps(final_message, indent=2)}")
producer.send(KAFKA_TOPIC, value=final_message)
producer.flush()

print("\nData sent successfully!")