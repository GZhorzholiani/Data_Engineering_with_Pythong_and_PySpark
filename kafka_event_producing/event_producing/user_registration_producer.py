import sys
import logging
from uuid import uuid4
from kafka_event_producing.kafka_producer import KafkaProducer
from kafka_event_producing.event_classes.user_registration_event import UserRegistrationEvent
from kafka_event_producing.avro_serialization_manager import AvroSerializationManager
from Configs.config import producer_conf, schema_registry_conf
from confluent_kafka.serialization import SerializationContext, MessageField


sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "user_registration"

user_registration = UserRegistrationEvent()
avro_manager = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic, user_registration.to_dict)
string_serializer = avro_manager.string_serializer


if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)

    try:
        while True:
            user_data = user_registration()
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(user_data,
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
