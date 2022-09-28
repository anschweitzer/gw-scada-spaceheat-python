"""Proactor-internal messages wrappers of Scada message structures."""

import time
import typing
from typing import (
    List,
)

from pydantic import BaseModel

from proactor.message import Message, Header, KnownNames
from schema.enums.telemetry_name.spaceheat_telemetry_name_100 import TelemetryName
from schema.gs.gs_pwr import GsPwr
from schema.gs.gs_pwr_maker import GsPwr_Maker
from schema.gt.gt_dispatch_boolean_local.gt_dispatch_boolean_local import (
    GtDispatchBooleanLocal,
)
from schema.gt.gt_dispatch_boolean_local.gt_dispatch_boolean_local_maker import (
    GtDispatchBooleanLocal_Maker,
)
from schema.gt.gt_driver_booleanactuator_cmd.gt_driver_booleanactuator_cmd import (
    GtDriverBooleanactuatorCmd,
)
from schema.gt.gt_driver_booleanactuator_cmd.gt_driver_booleanactuator_cmd_maker import (
    GtDriverBooleanactuatorCmd_Maker,
)
from schema.gt.gt_sh_telemetry_from_multipurpose_sensor.gt_sh_telemetry_from_multipurpose_sensor import (
    GtShTelemetryFromMultipurposeSensor,
)
from schema.gt.gt_sh_telemetry_from_multipurpose_sensor.gt_sh_telemetry_from_multipurpose_sensor_maker import (
    GtShTelemetryFromMultipurposeSensor_Maker,
)
from schema.gt.gt_telemetry.gt_telemetry import GtTelemetry
from schema.gt.gt_telemetry.gt_telemetry_maker import GtTelemetry_Maker


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
        payload = typing.cast(GsPwr, GsPwr_Maker(power=power).tuple)
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


# TODO: Replace with generalized debug message


class ScadaDBGPing(BaseModel):
    number: int


# TODO: Replace with generalized debug message


class ScadaDBGPingMessage(Message[ScadaDBGPing]):
    def __init__(
        self,
        number: int,
    ):
        super().__init__(
            header=Header(
                src="foo",
                dst=KnownNames.proactor.value,
                message_type=self.__class__.__name__,
            ),
            payload=ScadaDBGPing(number=number),
        )


# TODO: Replace with generalized debug message


class ShowSubscriptions(BaseModel):
    pass


# TODO: Replace with generalized debug message


class ShowSubscriptionsMessage(Message[ShowSubscriptions]):
    def __init__(self):
        super().__init__(
            header=Header(
                src="foo",
                dst=KnownNames.proactor.value,
                message_type=self.__class__.__name__,
            ),
            payload=ShowSubscriptions(),
        )
