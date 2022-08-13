import asyncio
from abc import ABC, abstractmethod

from proactor.message import Message


class CommunicatorInterface(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _send(self, message: Message):
        raise NotImplementedError

    @abstractmethod
    async def process_message(self, message: Message):
        raise NotImplementedError


class Communicator(CommunicatorInterface, ABC):
    _name: str
    _services: "ServicesInterface"

    def __init__(self, name: str, services: "ServicesInterface"):
        self._name = name
        self._services = services

    @property
    def name(self) -> str:
        return self._name

    def _send(self, message: Message):
        self._services.send(message)


class Runnable(ABC):
    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    @abstractmethod
    async def join(self):
        raise NotImplementedError

    async def stop_and_join(self):
        self.stop()
        await self.join()

class ServicesInterface(CommunicatorInterface):
    @abstractmethod
    def get_communicator(self, name: str) -> CommunicatorInterface:
        raise NotImplementedError

    @abstractmethod
    def send(self, message: Message):
        raise NotImplementedError

    @abstractmethod
    def send_threadsafe(self, message: Message) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def async_receive_queue(self) -> asyncio.Queue:
        raise NotImplementedError

