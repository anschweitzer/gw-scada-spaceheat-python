"""Test cloud_ear module"""

import time
from test.utils import EarRecorder
from test.utils import ScadaRecorder
from test.utils import wait_for

import load_house
from actors.simple_sensor import SimpleSensor
from actors.utils import gw_mqtt_topic_encode
from config import ScadaSettings
from schema import property_format
from schema.enums import TelemetryName
from schema.messages import GtShSimpleTelemetryStatus
from schema.messages import GtShStatus
from schema.messages import GtShStatus_Maker


def test_scada_ear_connection():
    settings = ScadaSettings()
    layout = load_house.load_all(settings)
    scada = ScadaRecorder("a.s", settings=settings, hardware_layout=layout)
    ear = EarRecorder(settings=settings, hardware_layout=layout)
    thermo0_node = layout.node("a.tank.temp0")
    thermo0 = SimpleSensor(thermo0_node.alias, settings=settings, hardware_layout=layout)
    try:

        scada.start()
        scada.terminate_main_loop()
        scada.main_thread.join()
        wait_for(scada.client.is_connected, 1)
        wait_for(scada.gw_client.is_connected, 1)
        ear.start()
        ear.terminate_main_loop()
        ear.main_thread.join()
        wait_for(ear.gw_client.is_connected, 1)

        thermo0.start()
        thermo0.terminate_main_loop()
        thermo0.main_thread.join()
        thermo0.update_telemetry_value()
        assert thermo0.telemetry_value is not None
        thermo0.report_telemetry()

        def _scada_received_telemetry() -> bool:
            return scada.num_received_by_topic["a.tank.temp0/gt.telemetry.110"] > 0

        wait_for(_scada_received_telemetry, 5)
        for unix_ms in scada.recent_simple_read_times_unix_ms[thermo0_node]:
            assert property_format.is_reasonable_unix_time_ms(unix_ms)
        single_status = scada.make_simple_telemetry_status(thermo0_node)
        assert isinstance(single_status, GtShSimpleTelemetryStatus)
        assert single_status.TelemetryName == scada.config[thermo0_node].reporting.TelemetryName
        assert (
            single_status.ReadTimeUnixMsList == scada.recent_simple_read_times_unix_ms[thermo0_node]
        )
        assert single_status.ValueList == scada.recent_simple_values[thermo0_node]
        assert single_status.ShNodeAlias == thermo0_node.alias

        scada.send_status()
        time.sleep(1)
        # why doesn't this work??
        # wait_for(ear.num_received > 0, 5)
        assert ear.num_received > 0
        scada_g_node_alias = scada.scada_g_node_alias
        assert ear.num_received_by_topic[gw_mqtt_topic_encode(
            f"{scada_g_node_alias}/{GtShStatus_Maker.type_alias}")] == 1
        assert isinstance(ear.payloads[-2], GtShStatus)
        simple_telemetry_list = ear.payloads[-2].SimpleTelemetryList
        assert len(simple_telemetry_list) > 0
        node_alias_list = list(map(lambda x: x.ShNodeAlias, simple_telemetry_list))
        assert "a.tank.temp0" in node_alias_list
        thermo0_idx = node_alias_list.index("a.tank.temp0")
        telemetry_name_list = list(map(lambda x: x.TelemetryName, simple_telemetry_list))
        assert telemetry_name_list[thermo0_idx] == TelemetryName.WATER_TEMP_F_TIMES1000
        assert ear.payloads[-2].ReportingPeriodS == 300

    finally:
        # noinspection PyBroadException
        try:
            scada.stop()
            ear.stop()
            thermo0.stop()
        except:
            pass
