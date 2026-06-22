# create_topics.py — create topics with intentional partition counts
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import config

admin = KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS)

topics = [
    NewTopic(config.TOPIC_READINGS,   config.READINGS_PARTITIONS, replication_factor=1),
    NewTopic(config.TOPIC_AGGREGATES, config.DERIVED_PARTITIONS,  replication_factor=1),
    NewTopic(config.TOPIC_ALERTS,     config.DERIVED_PARTITIONS,  replication_factor=1),
]

for t in topics:
    try:
        admin.create_topics([t])
        print(f"created {t.name} ({t.num_partitions} partitions)")
    except TopicAlreadyExistsError:
        print(f"exists  {t.name}")

admin.close()
