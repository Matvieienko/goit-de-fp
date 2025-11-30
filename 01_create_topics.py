from confluent_kafka.admin import AdminClient, NewTopic
from colorama import Fore, Style

# Конфігурація

MY_NAME = "fp_matvieienko"
kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Назви топіків
topic_in = f"{MY_NAME}_athlete_event_results"
topic_out = f"{MY_NAME}_enriched_athlete_avg"

# Налаштування клієнта
admin_client = AdminClient({
    "bootstrap.servers": kafka_config["bootstrap_servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanisms": kafka_config["sasl_mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"],
})

# Параметри топіків
num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(topic_in, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(topic_out, num_partitions=num_partitions, replication_factor=replication_factor),
]

# Створення
try:
    futures = admin_client.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(Fore.GREEN + f"Topic '{topic}' created successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.YELLOW + f"Topic '{topic}' likely already exists or failed: {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"Global error: {e}" + Style.RESET_ALL)