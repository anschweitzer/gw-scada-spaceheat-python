"""Test Scada2"""
import asyncio
import logging
import time
import typing

import pytest

import actors2
import load_house
from actors.scada import ScadaCmdDiagnostic
from actors2 import Scada2
from actors2.message import ShowSubscriptionsMessage
from actors2.nodes import Nodes
from config import ScadaSettings
from data_classes.sh_node import ShNode
from named_tuples.telemetry_tuple import TelemetryTuple
from schema.enums.telemetry_name.spaceheat_telemetry_name_100 import TelemetryName
from schema.gt.gt_sh_booleanactuator_cmd_status.gt_sh_booleanactuator_cmd_status import (
    GtShBooleanactuatorCmdStatus,
)
from schema.gt.snapshot_spaceheat.snapshot_spaceheat_maker import SnapshotSpaceheat


from schema.gt.gt_sh_multipurpose_telemetry_status.gt_sh_multipurpose_telemetry_status import (
    GtShMultipurposeTelemetryStatus,
)
from schema.gt.gt_sh_simple_telemetry_status.gt_sh_simple_telemetry_status import (
    GtShSimpleTelemetryStatus,
)
from test.utils import AtnRecorder, await_for


def test_scada_small():
    settings = ScadaSettings()
    load_house.load_all(settings.world_root_alias)
    scada = Scada2(node=ShNode.by_alias["a.s"], settings=settings, actors=dict())
    meter_node = ShNode.by_alias["a.m"]
    relay_node = ShNode.by_alias["a.elt1.relay"]
    temp_node = ShNode.by_alias["a.tank.temp0"]
    assert list(scada._data.recent_ba_cmds.keys()) == Nodes.my_boolean_actuators()
    assert list(scada._data.recent_ba_cmd_times_unix_ms.keys()) == Nodes.my_boolean_actuators()
    assert list(scada._data.latest_simple_value.keys()) == Nodes.my_simple_sensors()
    assert list(scada._data.recent_simple_values.keys()) == Nodes.my_simple_sensors()
    assert list(scada._data.recent_simple_read_times_unix_ms.keys()) == Nodes.my_simple_sensors()
    assert list(scada._data.latest_value_from_multipurpose_sensor.keys()) == Nodes.my_telemetry_tuples()
    assert list(scada._data.recent_values_from_multipurpose_sensor.keys()) == Nodes.my_telemetry_tuples()
    assert (
        list(scada._data.recent_read_times_unix_ms_from_multipurpose_sensor.keys())
        == Nodes.my_telemetry_tuples()
    )

    ###########################################
    # Testing making status messages
    ###########################################

    s = scada._data.make_simple_telemetry_status(node=typing.cast(ShNode, "garbage"))
    assert s is None

    scada._data.recent_simple_read_times_unix_ms[temp_node] = [int(time.time() * 1000)]
    scada._data.recent_simple_values[temp_node] = [63000]
    s = scada._data.make_simple_telemetry_status(temp_node)
    assert isinstance(s, GtShSimpleTelemetryStatus)

    tt = TelemetryTuple(
        AboutNode=ShNode.by_alias["a.elt1"],
        SensorNode=ShNode.by_alias["a.m"],
        TelemetryName=TelemetryName.CURRENT_RMS_MICRO_AMPS,
    )
    scada._data.recent_values_from_multipurpose_sensor[tt] = [72000]
    scada._data.recent_read_times_unix_ms_from_multipurpose_sensor[tt] = [int(time.time() * 1000)]
    s = scada._data.make_multipurpose_telemetry_status(tt=tt)
    assert isinstance(s, GtShMultipurposeTelemetryStatus)
    s = scada._data.make_multipurpose_telemetry_status(tt=typing.cast(TelemetryTuple, "garbage"))
    assert s is None

    scada._data.recent_ba_cmds[relay_node] = []
    scada._data.recent_ba_cmd_times_unix_ms[relay_node] = []

    # returns None if asked make boolean actuator status for
    # a node that is not a boolean actuator

    s = scada._data.make_booleanactuator_cmd_status(meter_node)
    assert s is None

    s = scada._data.make_booleanactuator_cmd_status(relay_node)
    assert s is None

    scada._data.recent_ba_cmds[relay_node] = [0]
    scada._data.recent_ba_cmd_times_unix_ms[relay_node] = [int(time.time() * 1000)]
    s = scada._data.make_booleanactuator_cmd_status(relay_node)
    assert isinstance(s, GtShBooleanactuatorCmdStatus)

    scada.send_status()

    ##################################
    # Testing actuation
    ##################################

    # test that turn_on and turn_off only work for boolean actuator nodes

    result = scada.turn_on(meter_node)
    assert result == ScadaCmdDiagnostic.DISPATCH_NODE_NOT_BOOLEAN_ACTUATOR
    result = scada.turn_off(meter_node)
    assert result == ScadaCmdDiagnostic.DISPATCH_NODE_NOT_BOOLEAN_ACTUATOR

    #################################
    # Other SCADA small tests
    ##################################

    scada._last_status_second = int(time.time() - 400)
    assert scada.time_to_send_status() is True

@pytest.mark.asyncio
async def test_scada2_relay_dispatch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logging.basicConfig(level="DEBUG")
    debug_logs_path = tmp_path / "output/debug_logs"
    debug_logs_path.mkdir(parents=True, exist_ok=True)
    settings = ScadaSettings(logging_on=True, log_message_summary=True)
    load_house.load_all(settings.world_root_alias)
    atn = AtnRecorder(node=ShNode.by_alias["a"], settings=settings)
    scada2 = Scada2(ShNode.by_alias["a.s"], settings, actors=dict())
    relay_alias = "a.elt1.relay"
    relay_node = ShNode.by_alias[relay_alias]
    relay2 = actors2.BooleanActuator(node=relay_node, services=scada2)
    # TODO: There should be some test-public way to do this
    # noinspection PyProtectedMember
    scada2._add_communicator(relay2)
    scada2._scada_atn_fast_dispatch_contract_is_alive_stub = True
    # TODO: Work out how this fits with run_forever()
    scada2.start()
    run_forever = asyncio.create_task(scada2.run_forever(), name="run_forever")
    try:
        atn.start()
        await await_for(
            atn.gw_client.is_connected,
            1,
            "waiting for atn connect",
        )

        # TODO provide clean test access
        await await_for(
            lambda: scada2._mqtt_clients.subscribed(scada2.GRIDWORKS_MQTT),
            3,
            "waiting for scada connect",
        )
        scada2.send_threadsafe(ShowSubscriptionsMessage())

        # Verify relay is off
        assert atn.latest_snapshot_payload is None
        atn.status()
        await await_for(
            lambda : atn.latest_snapshot_payload is not None,
            3,
            "atn did not receive first status"
        )
        snapshot1: SnapshotSpaceheat = typing.cast(SnapshotSpaceheat, atn.latest_snapshot_payload)
        assert isinstance(snapshot1, SnapshotSpaceheat)
        if snapshot1.Snapshot.AboutNodeAliasList:
            relay_idx = snapshot1.Snapshot.AboutNodeAliasList.index(relay_alias)
            relay_value = snapshot1.Snapshot.ValueList[relay_idx]
            assert relay_value is None or relay_value == 0
        assert (
            relay_node not in scada2._data.latest_simple_value or
            scada2._data.latest_simple_value[relay_node] != 1
        )

        atn.turn_on(relay_node)
        await await_for(
            lambda : scada2._data.latest_simple_value[relay_node] == 1,
            3,
            "scada did not receive update from relay"
        )

        # Check relay state
        atn.status()
        await await_for(
            lambda : atn.latest_snapshot_payload is not None and id(atn.latest_snapshot_payload) != id(snapshot1),
            3,
            "atn did not receive status"
        )
        snapshot2 = atn.latest_snapshot_payload
        assert isinstance(snapshot2, SnapshotSpaceheat)
        assert relay_alias in snapshot2.Snapshot.AboutNodeAliasList, f"ERROR relay [{relay_alias}] not in {snapshot2.Snapshot.AboutNodeAliasList}"
        relay_idx = snapshot2.Snapshot.AboutNodeAliasList.index(relay_alias)
        relay_value = snapshot2.Snapshot.ValueList[relay_idx]
        assert relay_value == 1

    finally:
        # noinspection PyBroadException
        try:
            await scada2.stop_and_join()
        except:
            pass

        # noinspection PyBroadException
        try:
            if not run_forever.done():
                run_forever.cancel()
        except:
            pass

        # noinspection PyBroadException
        try:
            atn.stop()
        except:
            pass
