from confluent_kafka.schema_registry import SchemaRegistryClient
from Configs.config import schema_registry_conf

def get_latest_schema(subject: str) -> str:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    latest_version = schema_registry_client.get_latest_version(subject)
    schema = latest_version.schema.schema_str
    return schema


