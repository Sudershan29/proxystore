"""Kafka connector implementation."""
from __future__ import annotations

import sys
import uuid
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import kafka
from kafka.errors import UnknownTopicOrPartitionError


class KafkaKey(NamedTuple):
    """Key representing the topic name in Kafka server.

    Attributes:
        kafka_key: Unique object ID.
    """

    kafka_key: str


class KafkaConnector:
    """Kafka server connector.

    Args:
        hostname: Kafka server hostname.
        clear: Remove all keys from the Redis server when
            [`close()`][proxystore.connectors.redis.RedisConnector.close]
            is called. This will delete keys regardless of if they were
            created by ProxyStore or not.
    """

    # TODO: Change it to a str | list[str] ! NOTE: A list of hostnames helps with better discover of the brokers
    def __init__(self, hostname: str = 'localhost', port: int = 9092, clear: bool = False) -> None:
        self.hostname           = hostname
        self.port               = port
        self._bootstrap_servers = [f"{hostname}:{port}"]
        self._producer_client   = kafka.KafkaProducer(bootstrap_servers=self._bootstrap_servers)
        self._admin_client      = kafka.KafkaAdminClient(bootstrap_servers=self._bootstrap_servers) # NOTE : Unstable interface | But using for delete topics
        self.clear              = clear
        self._group_id          = str(uuid.uuid4())

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(bootstrap_server={self._bootstrap_servers}'
        )

    def close(self, clear: bool | None = None) -> None:
        """Close the connector and clean up.
        """
        # NOTE: clear cannot work the same way I think, because 
        self._producer_client.flush()
        self._producer_client.close()
        self._admin_client.close()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'hostname': self.hostname,
            'port': self.port,
            'clear': self.clear,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> KafkaConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: KafkaKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        try:
            self._admin_client.delete_topics(topics=[key.kafka_key])
        except UnknownTopicOrPartitionError as e:
            return


    def exists(self, key: KafkaKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        resp = self._admin_client.describe_topics(topics=[key.kafka_key])
        if isinstance(resp, list) and len(resp) >= 1:
            resp = resp[0]
            return resp.get('error_code') == 0
        else:
            raise ValueError("Unable to connect with Kafka")
        
        

    def get(self, key: KafkaKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `{}` if the topic is empty.
        """
        consumer = kafka.KafkaConsumer(key.kafka_key,
                                       group_id=self._group_id,
                                       bootstrap_servers=self._bootstrap_servers,
                                       max_poll_records=1,
                                       auto_offset_reset='earliest')
        response = consumer.poll(timeout_ms=1000)
        if not response:
            result = None
        else:
            for topic_response in response.values():
                for message in topic_response:
                    result = message.value
                    break
        consumer.close()
        return result
        

    def get_batch(self, keys: Sequence[KafkaKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]
            
    def put(self, obj: bytes) -> KafkaKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = KafkaKey(kafka_key=str(uuid.uuid4()))
        self._producer_client.send(key.kafka_key, obj)
        # self._producer_client.flush()
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[KafkaKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

    def append(self, key: KafkaKey, obj: bytes) -> None:
        """Appends obj to an existing topic in the kafka store

        Args:
            key: The topic to append the key into
            obj: Sequence of serialized objects to put in the store.
        """
        self._producer_client.send(key.kafka_key, obj)

