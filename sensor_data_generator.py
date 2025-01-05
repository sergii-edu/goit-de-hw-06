from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topic_prefix

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

building_sensors = f"{topic_prefix}_building_sensors"
building_alerts = f"{topic_prefix}_building_alerts"
num_partitions = 2
replication_factor = 1

building_sensors_topic_in = NewTopic(
    name=building_sensors,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)
building_sensors_topic_out = NewTopic(
    name=building_alerts,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

try:
    admin_client.delete_topics(
        topics=[
            f"{topic_prefix}_building_sensors",
            f"{topic_prefix}_building_alerts",
        ]
    )
except Exception as e:
    print(f"An error occurred: {e}")


try:
    admin_client.create_topics(
        new_topics=[building_sensors_topic_in, building_sensors_topic_out],
        validate_only=False,
    )
except Exception as e:
    print(f"An error occurred: {e}")

for topic in admin_client.list_topics():
    if topic_prefix in topic:
        print(f"Topic '{topic}' created")

admin_client.close()
