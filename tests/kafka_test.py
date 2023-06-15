from __future__ import annotations

import uuid, pickle
from proxystore.connectors.kafka import KafkaConnector, KafkaKey


def test_presence_of_kafka_topic() -> None:
    """
        Checks if the Admin Client can create and check if topic persists
    """
    connector = KafkaConnector()
    key = connector.put(b'value')
    assert connector.exists(key)

    random_key = KafkaKey(kafka_key=str(uuid.uuid4()))
    assert not connector.exists(random_key)
    connector.close()


def test_streaming() -> None:
    """
        Checks if the stream has all the information
    """
    connector = KafkaConnector()
    N = 5
    key = connector.put(pickle.dumps(0))
    for i in range(1, N):
        connector.append(key, pickle.dumps(i))

    sum = 0
    for i in range(N):
        obj = connector.get(key)
        sum += int(pickle.loads(obj))

    assert sum == N * (N - 1)/2

def test_globus_submit() -> None:
    pass
