from typing import Dict, List

from schema.gs.gs_dispatch_maker import GsDispatch_Maker
from gwproto0 import GsPwr_Maker
from gwproto0 import GtDispatchBoolean_Maker
from gwproto0 import (
    GtDispatchBooleanLocal_Maker,
)
from gwproto0 import (
    GtDriverBooleanactuatorCmd_Maker,
)
from gwproto0 import GtShCliAtnCmd_Maker
from gwproto0 import (
    TelemetrySnapshotSpaceheat_Maker
)


from gwproto0 import (
    GtShStatus_Maker,
)

from gwproto0 import SnapshotSpaceheat_Maker
from gwproto0 import (
    GtShTelemetryFromMultipurposeSensor_Maker,
)
from gwproto0 import GtTelemetry_Maker

TypeMakerByAliasDict: Dict[str, GtTelemetry_Maker] = {}
schema_makers: List[GtTelemetry_Maker] = [
    GsDispatch_Maker,
    GsPwr_Maker,
    GtDispatchBoolean_Maker,
    GtDispatchBooleanLocal_Maker,
    GtDriverBooleanactuatorCmd_Maker,
    GtShCliAtnCmd_Maker,
    TelemetrySnapshotSpaceheat_Maker,
    GtShStatus_Maker,
    SnapshotSpaceheat_Maker,
    GtShTelemetryFromMultipurposeSensor_Maker,
    GtTelemetry_Maker,
]

for maker in schema_makers:
    TypeMakerByAliasDict[maker.type_alias] = maker

