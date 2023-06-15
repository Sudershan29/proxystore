from proxystore.store.base import Store
from proxystore import proxy
from proxystore.connectors.kafka import KafkaConnector

def test_proxy() -> None:
    store = Store('kafka', KafkaConnector())
    p, key = store.new_stream(10) # store.proxy(10)
    store.append_to_stream(12, key)

    assert proxy.extract(p) == 10
    assert proxy.extract(p) == 12
    assert proxy.extract(p) is None