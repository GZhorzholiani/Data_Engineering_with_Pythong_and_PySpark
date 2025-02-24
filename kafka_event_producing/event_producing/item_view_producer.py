import random
import sys
import logging
from uuid import uuid4
from kafka_event_producing.kafka_producer import KafkaProducer
from kafka_event_producing.event_classes.item_view_event import ItemViewEvent
from kafka_event_producing.avro_serialization_manager import AvroSerializationManager
from Configs.config import producer_conf, schema_registry_conf
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "item_view"

item_view = ItemViewEvent()
avro_manager = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic, item_view.to_dict)
string_serializer = avro_manager.string_serializer


if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)

    try:
        while True:

            with open("../user_ids.txt") as f:
                user_ids = f.read().splitlines()

                user_id = int(random.choice(user_ids))

            item_view_info = item_view(user_id)
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(item_view_info,
                                                            SerializationContext(topic, MessageField.VALUE))

            producer.produce_message(
                topic=topic,
                message_key=serialized_key,
                message_value=serialized_value
            )

    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
        producer.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        producer.close()