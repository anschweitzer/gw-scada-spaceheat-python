"""Sample driver script showing message in/out summary lines for a portion of the mqtt protocol."""

import sys
from typing import Optional, List

import dotenv

import load_house
from actors.boolean_actuator import BooleanActuator
from actors.power_meter import PowerMeter
from actors.simple_sensor import SimpleSensor
from command_line_utils import parse_args, setup_logging
from config import ScadaSettings
from data_classes.sh_node import ShNode
from utils import ScadaRecorder, AtnRecorder, HomeAloneRecorder, wait_for


def show_protocol(argv: Optional[List[str]] = None):
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)
    settings = ScadaSettings(_env_file=dotenv.find_dotenv(args.env_file), log_message_summary=True)
    setup_logging(args, settings)

    load_house.load_all(settings.world_root_alias)
    scada = ScadaRecorder(node=ShNode.by_alias["a.s"], settings=settings)
    atn = AtnRecorder(node=ShNode.by_alias["a"], settings=settings)
    home_alone = HomeAloneRecorder(node=ShNode.by_alias["a.home"], settings=settings)
    elt_relay = BooleanActuator(ShNode.by_alias["a.elt1.relay"], settings=settings)
    meter = PowerMeter(node=ShNode.by_alias["a.m"], settings=settings)
    thermo = SimpleSensor(node=ShNode.by_alias["a.tank.temp0"], settings=settings)
    actors = [scada, atn, home_alone, elt_relay, meter, thermo]

    try:
        print()
        print("## STARTING ###########################################################################################")
        for actor in actors:
            actor.start()
        for actor in actors:
            if hasattr(actor, "client"):
                wait_for(
                    actor.client.is_connected,
                    1,
                    tag=f"ERROR waiting for {actor.node.alias} client connect",
                )
            if hasattr(actor, "gw_client"):
                wait_for(
                    actor.gw_client.is_connected,
                    1,
                    "ERROR waiting for gw_client connect",
                )
        print("## CONNECTED ##########################################################################################")
        scada._scada_atn_fast_dispatch_contract_is_alive_stub = True

        print()
        print("## TURNING ON #########################################################################################")
        atn.turn_on(ShNode.by_alias["a.elt1.relay"])
        wait_for(lambda: elt_relay.relay_state == 1, 10, f"Relay state {elt_relay.relay_state}")
        print("## TURNED ON ##########################################################################################")

        print()
        print("## REQUESTING STATUS ##################################################################################")
        atn.status()
        wait_for(
            lambda: atn.cli_resp_received > 0, 10, f"cli_resp_received == 0 {atn.summary_str()}"
        )

        wait_for(
            lambda: scada.num_received_by_topic["a.elt1.relay/gt.telemetry.110"] > 0,
            10,
            f"scada elt telemetry. {scada.summary_str()}",
        )

        # wait_for(lambda: scada.num_received_by_topic["a.m/p"] > 0, 10, f"scada power. {scada.summary_str()}")
        # This should report after turning on the relay. But that'll take a simulated element
        # that actually turns on and can be read by the simulated power meter

        wait_for(
            lambda: scada.num_received_by_topic["a.tank.temp0/gt.telemetry.110"] > 0,
            10,
            f"scada temperature. {scada.summary_str()}",
        )
        print("## SCADA GOT STATUS ###################################################################################")

        print()
        print("## TURNING OFF ########################################################################################")
        atn.turn_off(ShNode.by_alias["a.elt1.relay"])
        wait_for(
            lambda: int(elt_relay.relay_state) == 0, 10, f"Relay state {elt_relay.relay_state}"
        )
        print("## TURNED OFF #########################################################################################")

        print()
        print("## SCADA SENDING STATUS ###############################################################################")
        scada.send_status()
        wait_for(lambda: atn.status_received > 0, 10, f"atn summary. {atn.summary_str()}")
        wait_for(
            lambda: home_alone.status_received > 0,
            10,
            f"home alone summary. {home_alone.summary_str()}",
        )
        print("## SCADA STATUS RECEIVED ##############################################################################")

    finally:
        for actor in actors:
            # noinspection PyBroadException
            try:
                actor.stop()
            except:
                pass


if __name__ == "__main__":
    show_protocol()