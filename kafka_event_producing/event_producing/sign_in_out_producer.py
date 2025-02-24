import random
import sys
import logging
from datetime import datetime, timedelta
from uuid import uuid4
from kafka_event_producing.kafka_producer import KafkaProducer
from kafka_event_producing.event_classes.sign_in_event import SignInEvent
from kafka_event_producing.event_classes.sign_out_event import SignOutEvent
from kafka_event_producing.avro_serialization_manager import AvroSerializationManager
from Configs.config import producer_conf, schema_registry_conf
from confluent_kafka.serialization import SerializationContext, MessageField


sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic_sign_in = "sign_in"
sign_in = SignInEvent()
avro_manager_sign_in = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic_sign_in, sign_in.to_dict)
string_serializer_sign_in = avro_manager_sign_in.string_serializer

topic_sign_out = "sign_out"
sign_out = SignOutEvent()
avro_manager_sign_out = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic_sign_out, sign_out.to_dict)
string_serializer_sign_out = avro_manager_sign_out.string_serializer


if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)

    try:
        while True:

            with open("../user_ids.txt") as f:
                user_ids = f.read().splitlines()

                user_id = int(random.choice(user_ids))

            sign_in_data = sign_in(user_id)
            serialized_key_sign_in = string_serializer_sign_in(str(uuid4()))
            serialized_value_sign_in = avro_manager_sign_in.avro_serializer(sign_in_data,
                                                            SerializationContext(topic_sign_in, MessageField.VALUE))
            producer.produce_message(
                topic=topic_sign_in,
                message_key=serialized_key_sign_in,
                message_value=serialized_value_sign_in
            )

            sign_in_timestamp = datetime.fromisoformat(sign_in_data["timestamp"])

            random_minutes = random.randint(1, 59)

            sign_out_timestamp = sign_in_timestamp + timedelta(minutes=random_minutes)

            sign_out_info = sign_out(user_id, sign_out_timestamp)
            serialized_key_sign_out = string_serializer_sign_out(str(uuid4()))
            serialized_value_sign_out = avro_manager_sign_out.avro_serializer(sign_out_info,
                                                            SerializationContext(topic_sign_out, MessageField.VALUE))
            producer.produce_message(
                topic=topic_sign_out,
                message_key=serialized_key_sign_out,
                message_value=serialized_value_sign_out
            )


    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
        producer.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        producer.close()
