import random
import sys
import logging
from uuid import uuid4
from kafka_event_producing.kafka_producer import KafkaProducer
from kafka_event_producing.event_classes.added_to_cart_event import AddedToCartEvent
from kafka_event_producing.event_classes.check_out_event import CheckOutEvent
from kafka_event_producing.avro_serialization_manager import AvroSerializationManager
from Configs.config import producer_conf, schema_registry_conf
from confluent_kafka.serialization import SerializationContext, MessageField


sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic_add_to_cart = 'add_to_cart'
added_to_cart = AddedToCartEvent()
avro_manager_add_to_cart = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic_add_to_cart, added_to_cart.to_dict)
string_serializer_add_to_cart = avro_manager_add_to_cart.string_serializer

topic_check_out = 'check_out'
check_out = CheckOutEvent()
avro_manager_check_out = AvroSerializationManager(schema_registry_conf['url'], schema_registry_conf['basic.auth.user.info'], topic_check_out, check_out.to_dict)
string_serializer_check_out = avro_manager_check_out.string_serializer


if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)

    try:
        while True:
            with open("../user_ids.txt") as f:
                user_ids = f.read().splitlines()

                user_id = int(random.choice(user_ids))

            added_to_cart_data = added_to_cart(user_id)
            serialized_key_add_to_cart = string_serializer_add_to_cart(str(uuid4()))
            serialized_value_add_to_cart = avro_manager_add_to_cart.avro_serializer(added_to_cart_data,
                                                            SerializationContext(topic_add_to_cart, MessageField.VALUE))
            producer.produce_message(
                topic=topic_add_to_cart,
                message_key=serialized_key_add_to_cart,
                message_value=serialized_value_add_to_cart
            )
            cart_id = added_to_cart_data["cart_id"]
            added_to_cart_data = check_out(user_id, cart_id)
            serialized_key_check_out = string_serializer_check_out(str(uuid4()))
            serialized_value_check_out = avro_manager_check_out.avro_serializer(added_to_cart_data,
                                                            SerializationContext(topic_check_out, MessageField.VALUE))
            producer.produce_message(
                topic=topic_check_out,
                message_key=serialized_key_check_out,
                message_value=serialized_value_check_out
            )


    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
        producer.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        producer.close()
