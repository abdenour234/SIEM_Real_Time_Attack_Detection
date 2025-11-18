import os
import sys
import requests
from confluent_kafka import Producer
import json

# allow importing create_topic from project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from create_topic import ensure_topic

# Configuration du producteur Kafka
producer_config = {
    "bootstrap.servers": "localhost:9092"  # ⚠️ utilise 'kafka-new' car c'est le nom du conteneur Docker
}
producer = Producer(producer_config)

# Callback de livraison
def delivery_report(err, msg):
    """Delivery callback: prints failure or the event details on success.

    On success prints: event_id -> user_id -> partition -> offset
    """
    if err:
        print(f"❌ Delivery failed: {err}")
        return

    # Try to parse the message value (JSON) to extract event id and user_id
    event_id = ""
    user_id = ""
    try:
        raw = msg.value()
        if raw is None:
            parsed = {}
        else:
            if isinstance(raw, (bytes, bytearray)):
                parsed = json.loads(raw.decode("utf-8"))
            elif isinstance(raw, str):
                parsed = json.loads(raw)
            else:
                parsed = {}

        # common field names for id/user
        event_id = parsed.get("event_id") or parsed.get("id") or parsed.get("eventId") or ""
        user_id = parsed.get("user_id") or parsed.get("userId") or ""
    except Exception:
        # fallback to the message key if parsing fails
        try:
            key = msg.key()
            if key is not None:
                user_id = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key)
        except Exception:
            user_id = ""

    print(f"✅ Sent: {event_id} -> {user_id} -> partition:{msg.partition()} -> offset:{msg.offset()}")
    

# Fonction principale
def stream_and_produce():
    url = "http://localhost:8080/events/stream" # ⚠️ utilise 'host.docker.internal' pour accéder à l'hôte depuis Docker
    print(f"🚀 Connexion à {url} ...")

    
    topic_name = "data.stream.raw"

    # On se connecte à l'API en mode streaming
    with requests.get(url, stream=True) as response:
        if response.status_code != 200:
            print(f"❌ Erreur API: {response.status_code}")
            return
        # Lecture ligne par ligne
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode("utf-8").strip()
                
                # L'API renvoie des lignes commençant par "data: "
                if decoded_line.startswith("data: "):
                    data_str = decoded_line.replace("data: ", "")
                    try:
                        event = json.loads(data_str)
                        user_id = event.get("user_id", "")
                        producer.produce(
                            topic=topic_name,  # 🔹 nom du topic Kafka
                            key=str(user_id),
                            value=json.dumps(event).encode("utf-8"),
                            callback=delivery_report
                        )
                        producer.poll(0)
                    except json.JSONDecodeError:
                        print(f"⚠️ Ligne ignorée (non JSON): {data_str}")
    
    producer.flush()
    print("✅ Tous les messages ont été envoyés à Kafka.")

if __name__ == "__main__":
    stream_and_produce()
