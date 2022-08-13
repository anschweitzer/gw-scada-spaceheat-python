"""Proactor implementation"""

import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Awaitable, Any, Optional

from paho.mqtt.client import MQTTMessageInfo

import config
from actors.utils import MessageSummary
from proactor.message import (
    Message,
    KnownNames,
    MessageType,
    MQTTConnectPayload,
    MQTTReceiptPayload,
    MQTTConnectFailPayload,
    MQTTDisconnectPayload,
)
from proactor.mqtt import MQTTClients
from proactor.proactor_interface import ServicesInterface, Runnable, CommunicatorInterface
from proactor.sync_thread import AsyncQueueWriter


class MQTTCodec(ABC):

    @abstractmethod
    def encode(self, payload: Any) -> bytes:
        pass

    @abstractmethod
    def decode(self, receipt_payload: MQTTReceiptPayload) -> Any:
        pass




class Proactor(ServicesInterface, Runnable):
    _name: str
    _loop: asyncio.AbstractEventLoop
    _receive_queue: asyncio.Queue
    _mqtt_clients: MQTTClients
    _mqtt_codecs: Dict[str, MQTTCodec]
    _communicators: Dict[str, CommunicatorInterface]
    _stop_requested: bool
    _tasks: List[asyncio.Task]

    # TODO: Clean up loop control
    def __init__(self, name: str = KnownNames.proactor.value, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._name = name
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._receive_queue = asyncio.Queue(loop=loop)
        self._mqtt_clients = MQTTClients(AsyncQueueWriter(self._loop, self._receive_queue))
        self._mqtt_codecs = dict()
        self._communicators = dict()
        self._tasks = []
        self._stop_requested = False

    def _add_mqtt_client(
        self,
        name: str,
        client_config: config.MQTTClient,
        codec: Optional[MQTTCodec] = None,
    ):
        self._mqtt_clients.add_client(name, client_config)
        if codec is not None:
            self._mqtt_codecs[name] = codec

    def _encode_and_publish(
        self, client: str, topic: str, payload: Any, qos: int
    ) -> MQTTMessageInfo:
        print(MessageSummary.format("OUTq", client, topic, payload))
        encoder = self._mqtt_codecs[client]
        return self._mqtt_clients.publish(client, topic, encoder.encode(payload), qos)

    def _add_communicator(self, communicator:CommunicatorInterface):
        # TODO: There probably needs to be some public version of this for testing.
        if communicator.name in self._communicators:
            raise ValueError(f"ERROR. Communicator with name [{communicator.name}] already present")
        self._communicators[communicator.name] = communicator

    @property
    def async_receive_queue(self):
        return self._receive_queue

    async def process_messages(self):
        while not self._stop_requested:
            message = await self._receive_queue.get()
            if not self._stop_requested:
                await self.process_message(message)
            self._receive_queue.task_done()

    def start_tasks(self):
        self._tasks = [asyncio.create_task(self.process_messages())]
        self._start_derived_tasks()

    def _start_derived_tasks(self):
        pass

    async def _derived_process_message(self, message: Message):
        pass

    async def _derived_process_mqtt_message(self, message: Message[MQTTReceiptPayload], decoded: Any):
        pass

    async def process_message(self, message: Message):
        print(f"++Proactor.process_message {message.header.src}/{message.header.message_type}")
        path_dbg = 0
        print(MessageSummary.format("INx ", self.name, f"{message.header.src}/{message.header.message_type}", message.payload))
        if message.header.message_type == MessageType.mqtt_message.value:
            path_dbg |= 0x00000001
            await self._process_mqtt_message(message)
        elif message.header.message_type == MessageType.mqtt_connected.value:
            path_dbg |= 0x00000002
            self._process_mqtt_connected(message)
        elif message.header.message_type == MessageType.mqtt_disconnected.value:
            path_dbg |= 0x00000004
            self._process_mqtt_disconnected(message)
        elif message.header.message_type == MessageType.mqtt_connect_failed.value:
            path_dbg |= 0x00000008
            self._process_mqtt_connect_fail(message)
        else:
            path_dbg |= 0x00000010
            await self._derived_process_message(message)
        print(f"--Proactor.process_message  path:0x{path_dbg:08X}")

    async def _process_mqtt_message(self, message: Message[MQTTReceiptPayload]):
        print(f"++Proactor._process_mqtt_message {message.header.src}/{message.header.message_type}")
        path_dbg = 0
        decoder = self._mqtt_codecs.get(message.payload.client_name, None)
        if decoder is not None:
            path_dbg |= 0x00000001
            decoded = decoder.decode(message.payload)
        else:
            path_dbg |= 0x00000002
            decoded = message.payload
        print(MessageSummary.format("INq ", self.name, message.payload.message.topic, decoded))
        await self._derived_process_mqtt_message(message, decoded)
        print(f"--Proactor._process_mqtt_message  path:0x{path_dbg:08X}")

    def _process_mqtt_connected(self, message: Message[MQTTConnectPayload]):
        self._mqtt_clients.subscribe_all(message.payload.client_name)

    def _process_mqtt_disconnected(self, message: Message[MQTTDisconnectPayload]):
        pass

    def _process_mqtt_connect_fail(self, message: Message[MQTTConnectFailPayload]):
        pass

    async def run_forever(self):
        self.start_tasks()
        await self.join()

    def start_mqtt(self):
        self._mqtt_clients.start()

    def stop_mqtt(self):
        self._mqtt_clients.stop()

    def start(self):
        self.start_mqtt()
        for communicator in self._communicators.values():
            if isinstance(communicator, Runnable):
                communicator.start()

    def stop(self):
        self._stop_requested = True
        self.stop_mqtt()
        for communicator in self._communicators.values():
            if isinstance(communicator, Runnable):
                # noinspection PyBroadException
                try:
                    communicator.stop()
                except:
                    pass

    async def join(self):
        running: List[Awaitable] = self._tasks[:]
        for communicator in self._communicators.values():
            if isinstance(communicator, Runnable):
                running.append(communicator.join())
        running: List[Awaitable] = [
            entry.join()
            for entry in self._communicators.values()
            if isinstance(entry, Runnable)
        ]
        return await asyncio.gather(*running, loop=self._loop)

    def publish(self, client: str, topic: str, payload: bytes, qos: int):
        self._mqtt_clients.publish(client, topic, payload, qos)

    def send(self, message: Message):
        print(MessageSummary.format("OUTx",  message.header.src, f"{message.header.dst}/{message.header.message_type}", message.payload))
        self._receive_queue.put_nowait(message)

    def send_threadsafe(self, message: Message) -> None:
        self._loop.call_soon_threadsafe(
            self._receive_queue.put_nowait, message
        )

    def get_communicator(self, name: str) -> CommunicatorInterface:
        return self._communicators[name]

    @property
    def name(self) -> str:
        return self._name

    def _send(self, message: Message):
        self.send(message)
