from data_classes.sh_node import ShNode
from schema.enums.role.role_map import Role

from actors.atn import Atn
from actors.boolean_actuator import BooleanActuator
from actors.home_alone import HomeAlone
from actors.pipe_flow_meter import PipeFlowMeter
from actors.power_meter import PowerMeter
from actors.scada import Scada
from actors.tank_water_temp_sensor import TankWaterTempSensor

switcher = {
    Role.ATN: Atn,
    Role.BOOLEAN_ACTUATOR: BooleanActuator,
    Role.HOME_ALONE: HomeAlone,
    Role.PIPE_FLOW_METER: PipeFlowMeter,
    Role.POWER_METER: PowerMeter,
    Role.SCADA: Scada,
    Role.TANK_WATER_TEMP_SENSOR: TankWaterTempSensor,
}


def strategy_from_node(node: ShNode):
    if not node.has_actor:
        return None
    if node.role not in list(switcher.keys()):
        raise Exception(f"Missing implementation for {node.role.value}!")
    func = switcher[node.role]
    return func


def stickler():
    return None
