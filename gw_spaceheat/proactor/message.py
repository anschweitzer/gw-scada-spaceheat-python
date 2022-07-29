from enum import Enum
from typing import Any, Optional, TypeVar, Generic

from paho.mqtt.client import MQTTMessage
from pydantic import BaseModel
from pydantic.generics import GenericModel


class MessageType(Enum):
    invalid = "invalid"
    mqtt_subscribe = "mqtt_subscribe"
    mqtt_message = "mqtt_message"

    mqtt_connected = "mqtt_connected"
    mqtt_disconnected = "mqtt_disconnected"
    mqtt_connect_failed = "mqtt_connect_failed"


class KnownNames(Enum):
    proactor = "proactor"
    mqtt_clients = "mqtt_clients"


class Header(BaseModel):
    src: str
    dst: str
    message_type: str
    sequence_number: int


PayloadT = TypeVar("PayloadT")


class Message(GenericModel, Generic[PayloadT]):
    header: Header
    payload: PayloadT


class MQTTClientsPayload(BaseModel):
    client_name: str
    userdata: Optional[Any]


MQTTClientsPayloadT = TypeVar("MQTTClientsPayloadT", bound=MQTTClientsPayload)


class MQTTClientMessage(Message[MQTTClientsPayloadT], Generic[MQTTClientsPayloadT]):
    def __init__(
        self,
        message_type: MessageType,
        sequence_number: int,
        payload: MQTTClientsPayloadT,
    ):
        super().__init__(
            header=Header(
                src=KnownNames.mqtt_clients.value,
                dst=KnownNames.proactor.value,
                message_type=message_type.value,
                sequence_number=sequence_number,
            ),
            payload=payload,
        )


class MQTTMessageModel(BaseModel):
    timestamp: float = 0
    state: int = 0
    dup: bool = False
    mid: int = 0
    topic: str = ""
    payload: bytes = bytes()
    qos: int = 0
    retain: bool = False

    @classmethod
    def from_mqtt_message(cls, message: MQTTMessage) -> "MQTTMessageModel":
        model = MQTTMessageModel()
        for field_name in model.__fields__:
            setattr(model, field_name, getattr(message, field_name))
        return model


class MQTTReceiptPayload(MQTTClientsPayload):
    message: MQTTMessageModel


class MQTTReceiptMessage(MQTTClientMessage[MQTTReceiptPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        message: MQTTMessage,
        sequence_number: int,
    ):
        super().__init__(
            message_type=MessageType.mqtt_message,
            sequence_number=sequence_number,
            payload=MQTTReceiptPayload(
                client_name=client_name,
                userdata=userdata,
                message=MQTTMessageModel.from_mqtt_message(message),
            ),
        )


class MQTTCommEventPayload(MQTTClientsPayload):
    rc: int


class MQTTConnectPayload(MQTTCommEventPayload):
    flags: int


class MQTTConnectMessage(MQTTClientMessage[MQTTConnectPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        flags: int,
        rc: int,
        sequence_number: int,
    ):
        super().__init__(
            message_type=MessageType.mqtt_connected,
            sequence_number=sequence_number,
            payload=MQTTConnectPayload(
                client_name=client_name,
                userdata=userdata,
                flags=flags,
                rc=rc,
            ),
        )


class MQTTConnectFailPayload(MQTTClientsPayload):
    pass


class MQTTConnectFailMessage(MQTTClientMessage[MQTTConnectFailPayload]):
    def __init__(self, client_name: str, userdata: Optional[Any], sequence_number: int):
        super().__init__(
            message_type=MessageType.mqtt_connect_failed,
            sequence_number=sequence_number,
            payload=MQTTConnectFailPayload(
                client_name=client_name,
                userdata=userdata,
            ),
        )


class MQTTDisconnectPayload(MQTTCommEventPayload):
    pass


class MQTTDisconnectMessage(MQTTClientMessage[MQTTDisconnectPayload]):
    def __init__(
        self, client_name: str, userdata: Optional[Any], rc: int, sequence_number: int
    ):
        super().__init__(
            message_type=MessageType.mqtt_disconnected,
            sequence_number=sequence_number,
            payload=MQTTDisconnectPayload(
                client_name=client_name,
                userdata=userdata,
                rc=rc,
            ),
        )
