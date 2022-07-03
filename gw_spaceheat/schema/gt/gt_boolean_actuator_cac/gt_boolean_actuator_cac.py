"""gt.boolean.actuator.cac.100 type"""

from schema.errors import MpSchemaError
from schema.gt.gt_boolean_actuator_cac.gt_boolean_actuator_cac_base import (
    GtBooleanActuatorCacBase,
)


class GtBooleanActuatorCac(GtBooleanActuatorCacBase):
    def check_for_errors(self):
        errors = self.derived_errors() + self.hand_coded_errors()
        if len(errors) > 0:
            raise MpSchemaError(
                f" Errors making making gt.boolean.actuator.cac.100 for {self}: {errors}"
            )

    def hand_coded_errors(self):
        return []
