"""Proactor-internal messages wrappers of Scada message structures."""

import time
from typing import (
    List,
    Optional,
    Literal,
    cast
)
from enum import Enum

from pydantic import BaseModel, validator

from logging_config import LoggerLevels
from gwproto0 import Message, Header, as_enum
from gwproto0.enums.telemetry_name.telemetry_name_map import TelemetryName
from gwproto0 import GsPwr
from gwproto0 import GsPwr_Maker
from gwproto0 import (
    GtDispatchBooleanLocal,
)
from gwproto0 import (
    GtDispatchBooleanLocal_Maker,
)
from gwproto0 import (
    GtDriverBooleanactuatorCmd,
)
from gwproto0 import (
    GtDriverBooleanactuatorCmd_Maker,
)
from gwproto0 import (
    GtShTelemetryFromMultipurposeSensor,
)
from gwproto0 import (
    GtShTelemetryFromMultipurposeSensor_Maker,
)
from gwproto0 import GtTelemetry
from gwproto0 import GtTelemetry_Maker


class GtTelemetryMessage(Message[GtTelemetry]):
    def __init__(
        self,
        src: str,
        dst: str,
        telemetry_name: TelemetryName,
        value: int,
        exponent: int,
        scada_read_time_unix_ms: int,
    ):
        payload = GtTelemetry_Maker(
            name=telemetry_name,
            value=value,
            exponent=exponent,
            scada_read_time_unix_ms=scada_read_time_unix_ms,
        ).tuple
        super().__init__(
            header=Header(
                src=src,
                dst=dst,
                message_type=payload.TypeAlias,
            ),
            payload=payload,
        )


class GtDriverBooleanactuatorCmdResponse(Message[GtDriverBooleanactuatorCmd]):
    def __init__(
        self,
        src: str,
        dst: str,
        relay_state: int,
    ):
        payload = GtDriverBooleanactuatorCmd_Maker(
            relay_state=relay_state,
            command_time_unix_ms=int(time.time() * 1000),
            sh_node_alias=src,
        ).tuple
        super().__init__(
            header=Header(
                src=src,
                dst=dst,
                message_type=payload.TypeAlias,
            ),
            payload=payload,
        )


class GtDispatchBooleanLocalMessage(Message[GtDispatchBooleanLocal]):
    def __init__(
        self,
        src: str,
        dst: str,
        relay_state: int,
    ):
        payload = GtDispatchBooleanLocal_Maker(
            from_node_alias=src,
            about_node_alias=dst,
            relay_state=relay_state,
            send_time_unix_ms=int(time.time() * 1000),
        ).tuple
        super().__init__(
            header=Header(
                src=src,
                dst=dst,
                message_type=payload.TypeAlias,
            ),
            payload=payload,
        )


class GsPwrMessage(Message[GsPwr]):
    def __init__(
        self,
        src: str,
        dst: str,
        power: int,
    ):
        payload = cast(GsPwr, GsPwr_Maker(power=power).tuple)
        super().__init__(
            header=Header(
                src=src,
                dst=dst,
                message_type=payload.TypeAlias,
            ),
            payload=payload,
        )


class MultipurposeSensorTelemetryMessage(Message[GtShTelemetryFromMultipurposeSensor]):
    def __init__(
        self,
        src: str,
        dst: str,
        about_node_alias_list: List[str],
        value_list: List[int],
        telemetry_name_list: List[TelemetryName],
    ):
        payload = GtShTelemetryFromMultipurposeSensor_Maker(
            about_node_alias_list=about_node_alias_list,
            value_list=value_list,
            telemetry_name_list=telemetry_name_list,
            scada_read_time_unix_ms=int(1000 * time.time()),
        ).tuple
        super().__init__(
            header=Header(
                src=src,
                dst=dst,
                message_type=payload.TypeAlias,
            ),
            payload=payload,
        )


class ScadaDBGCommands(Enum):
    show_subscriptions = "show_subscriptions"

class ScadaDBG(BaseModel):
    levels: LoggerLevels = LoggerLevels(
        message_summary=-1,
        lifecycle=-1,
        comm_event=-1,
    )
    command: Optional[ScadaDBGCommands] = None
    type_name: Literal["gridworks.scada.dbg.000"] = "gridworks.scada.dbg.000"

    @validator("command", pre=True)
    def command_value(cls, v):
        return as_enum(v, ScadaDBGCommands)
