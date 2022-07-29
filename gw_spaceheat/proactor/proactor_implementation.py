import asyncio
from typing import Dict, List, Awaitable

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
from proactor.proactor_interface import ServicesInterface, Communicator, Runnable


class Proactor(ServicesInterface, Runnable):
    _name: str
    _receive_queue: asyncio.Queue
    _mqtt_clients: MQTTClients
    _communicators: Dict[str, Communicator]
    _stop_requested: bool
    _tasks: List[asyncio.Task]

    def __init__(self, name: str = KnownNames.proactor.value):
        self._name = name
        self._receive_queue = asyncio.Queue()
        self._mqtt_clients = MQTTClients(self._receive_queue)
        self._communicators = dict()
        self._tasks = []
        self._stop_requested = False

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

    async def process_message(self, message: Message):
        # Validate
        # Decode
        # Demultiplex
        # Process
        if message.header.message_type == MessageType.mqtt_message.value:
            self._process_mqtt_message(message)
        elif message.header.message_type == MessageType.mqtt_connected.value:
            self._process_mqtt_connected(message)
        elif message.header.message_type == MessageType.mqtt_disconnected.value:
            self._process_mqtt_disconnected(message)
        elif message.header.message_type == MessageType.mqtt_connect_failed.value:
            self._process_mqtt_connect_fail(message)
        else:
            await self._derived_process_message(message)

    def _process_mqtt_message(self, message: Message[MQTTReceiptPayload]):
        pass

    def _process_mqtt_connected(self, message: Message[MQTTConnectPayload]):
        self._mqtt_clients.subscribe_all(message.payload.client_name)

    def _process_mqtt_disconnected(self, message: Message[MQTTDisconnectPayload]):
        pass

    def _process_mqtt_connect_fail(self, message: Message[MQTTConnectFailPayload]):
        pass

    async def run_forever(self):
        pass

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
        return await asyncio.gather(*running)

    def publish(self, client: str, topic: str, payload: bytes, qos: int):
        self._mqtt_clients.publish(client, topic, payload, qos)

    def send(self, message: Message):
        self._receive_queue.put_nowait(message)

    def get_communicator(self, name: str) -> Communicator:
        return self._communicators[name]

    @property
    def name(self) -> str:
        return self._name

    def _send(self, message: Message):
        self.send(message)
