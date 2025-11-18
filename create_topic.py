"""create_topic.py
Helper to ensure a Kafka topic exists with the requested number of partitions.

Usage:
    from create_topic import ensure_topic
    ensure_topic("data.stream.raw", num_partitions=6)

Also runnable as a script to create/check a topic.
"""
from confluent_kafka.admin import AdminClient, NewTopic

# hdchy kaml t9ed diro b cammande dyl kafka (kafka-topics.sh --create --topic ... )
def ensure_topic(topic_name: str,
                 num_partitions: int = 6,
                 replication_factor: int = 1,
                 bootstrap_servers: str = "localhost:9092",
                 timeout: int = 10) -> bool:
    """Ensure the Kafka topic exists with the requested number of partitions.

    Returns True when topic exists or was created, False on failure.
    Prints informational messages about actions taken.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        md = admin.list_topics(timeout=5) # kanjdbo les topics les kynin f cluster kafka dylna (hd fcts ktred metadata mchy ghir topics (a verifié))
        if topic_name in md.topics: # hna knverifiw wch topic dylna kyn deja 
            tmeta = md.topics.get(topic_name) # knchofo metadata dyal topic dylna (b7al offser,num of partitions)
            part_count = len(tmeta.partitions) if tmeta is not None and tmeta.partitions is not None else None # hna knjbdo ch7al kyna mn partition
            print(f"ℹ️ Topic '{topic_name}' exists with {part_count} partitions")
            if part_count is not None and part_count != num_partitions:
                print(f"⚠️ Topic '{topic_name}' partition count ({part_count}) differs from expected ({num_partitions})")
            return True

        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)#f7alat mkntch kyna kncréewha
        fs = admin.create_topics([new_topic])
        f = fs.get(topic_name)
        # wait for the operation to finish or raise
        f.result(timeout=timeout)
        print(f"ℹ️ Topic '{topic_name}' created with {num_partitions} partitions")
        return True
    except Exception as e:
        print(f"⚠️ Topic creation/check failed: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create/check Kafka topic with partitions")
    parser.add_argument("--topic", default="data.stream.raw")
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication", type=int, default=1)
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    ok = ensure_topic(args.topic, num_partitions=args.partitions, replication_factor=args.replication, bootstrap_servers=args.bootstrap)
    if not ok:
        raise SystemExit(1)
