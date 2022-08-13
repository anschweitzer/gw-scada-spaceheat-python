from abc import ABC

from actors2.actor_interface import ActorInterface
from actors2.scada_interface import ScadaInterface
from data_classes.sh_node import ShNode
from proactor.proactor_interface import Communicator


class Actor(ActorInterface, Communicator, ABC):

    _node: ShNode

    def __init__(self, node: ShNode, services: ScadaInterface):
        super().__init__(node.alias, services)

    @property
    def services(self):
        return self._services

    @property
    def alias(self):
        return self._name

    def node(self):
        return self._node

