"""MQTT infrastructure providing support for multiple MTQTT clients

TODO: Replace synchronous use of Paho MQTT Client with asyncio usage, per Paho documentation or external library

Main current limitation: each interaction between asyncio code and the mqtt clients must either have thread locking
(as is provided inside paho for certain functions such as publish()) or an explicit message based API.

"""

import uuid
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

from paho.mqtt.client import Client as PahoMQTTClient, MQTTMessageInfo

import config
from proactor.message import (
    MQTTReceiptMessage,
    MQTTConnectMessage,
    MQTTConnectFailMessage,
    MQTTDisconnectMessage,
)
from proactor.sync_thread import AsyncQueueWriter

class MQTTClientWrapper:
    _name: str
    _client_config: config.MQTTClient
    _client: PahoMQTTClient
    _receive_queue: AsyncQueueWriter
    _subscriptions: Dict[str, int]

    def __init__(
        self,
        name: str,
        client_config: config.MQTTClient,
        receive_queue: AsyncQueueWriter,
    ):
        self.name = name
        self._client_config = client_config
        self._receive_queue = receive_queue
        self._client = PahoMQTTClient("-".join(str(uuid.uuid4()).split("-")[:-1]))
        self._client.username_pw_set(
            username=self._client_config.username,
            password=self._client_config.password.get_secret_value(),
        )
        self._client.on_message = self.on_message
        self._client.on_connect = self.on_connect
        self._client.on_connect_fail = self.on_connect_fail
        self._client.on_disconnect = self.on_disconnect
        self._subscriptions = dict()

    def start(self):
        self._client.connect(self._client_config.host, port=self._client_config.port)
        self._client.loop_start()

    def stop(self):
        self._client.disconnect()
        self._client.loop_stop()

    def publish(self, topic: str, payload: bytes, qos: int) -> MQTTMessageInfo:
        return self._client.publish(topic, payload, qos)

    def subscribe(self, topic: str, qos: int) -> Tuple[int, Optional[int]]:
        self._subscriptions[topic] = qos
        return self._client.subscribe(topic, qos)

    def subscribe_all(self) -> Tuple[int, Optional[int]]:
        if self._subscriptions:
            return self._client.subscribe(list(self._subscriptions.items()), 0)

    def unsubscribe(self, topic: str) -> Tuple[int, Optional[int]]:
        self._subscriptions.pop(topic, None)
        return self._client.unsubscribe(topic)

    def on_message(self, _, userdata, message):
        self._receive_queue.put(
            MQTTReceiptMessage(
                client_name=self.name,
                userdata=userdata,
                message=message,
            )
        )

    def on_connect(self, _, userdata, flags, rc):
        self._receive_queue.put(
            MQTTConnectMessage(
                client_name=self.name,
                userdata=userdata,
                flags=flags,
                rc=rc,
            )
        )

    def on_connect_fail(self, _, userdata):
        self._receive_queue.put(
            MQTTConnectFailMessage(
                client_name=self.name,
                userdata=userdata,
            )
        )

    def on_disconnect(self, _, userdata, rc):
        self._receive_queue.put(
            MQTTDisconnectMessage(
                client_name=self.name,
                userdata=userdata,
                rc=rc,
            )
        )

class MQTTClients:
    _clients: Dict[str, MQTTClientWrapper]
    _send_queue: AsyncQueueWriter
    _subscriptions: Dict[str, List[Tuple[str, int]]]

    def __init__(self, async_queue_writer: AsyncQueueWriter):
        self._send_queue = async_queue_writer
        self._clients = dict()
        self._subscriptions = defaultdict(list)

    def add_client(
        self,
        name: str,
        client_config: config.MQTTClient,
    ):
        if name in self._clients:
            raise ValueError(f"ERROR. MQTT client named {name} already exists")
        self._clients[name] = MQTTClientWrapper(name, client_config, self._send_queue)

    def publish(
        self, client: str, topic: str, payload: bytes, qos: int
    ) -> MQTTMessageInfo:
        return self._clients[client].publish(topic, payload, qos)

    def subscribe(self, client: str, topic: str, qos: int) -> Tuple[int, Optional[int]]:
        return self._clients[client].subscribe(topic, qos)

    def subscribe_all(self, client: str) -> Tuple[int, Optional[int]]:
        return self._clients[client].subscribe_all()

    def unsubscribe(self, client: str, topic: str) -> Tuple[int, Optional[int]]:
        return self._clients[client].unsubscribe(topic)

    def stop(self):
        for client in self._clients.values():
            client.stop()

    def start(self):
        for client in self._clients.values():
            client.start()
