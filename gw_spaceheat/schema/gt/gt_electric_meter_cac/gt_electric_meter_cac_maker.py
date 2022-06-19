"""Makes gt.electric.meter.cac type"""

import json
from typing import Dict, Optional
from data_classes.cacs.electric_meter_cac import ElectricMeterCac

from schema.gt.gt_electric_meter_cac.gt_electric_meter_cac import GtElectricMeterCac
from schema.errors import MpSchemaError
from schema.enums.make_model.make_model_map import MakeModel, MakeModelMap


class GtElectricMeterCac_Maker():
    type_alias = 'gt.electric.meter.cac.100'

    def __init__(self,
                 component_attribute_class_id: str,
                 make_model: MakeModel,
                 comms_method: Optional[str],
                 display_name: Optional[str]):

        tuple = GtElectricMeterCac(ComponentAttributeClassId=component_attribute_class_id,
                                          CommsMethod=comms_method,
                                          MakeModel=make_model,
                                          DisplayName=display_name,
                                          )
        tuple.check_for_errors()
        self.tuple: GtElectricMeterCac = tuple

    @classmethod
    def tuple_to_type(cls, tuple: GtElectricMeterCac) -> str:
        tuple.check_for_errors()
        return tuple.as_type()

    @classmethod
    def type_to_tuple(cls, t: str) -> GtElectricMeterCac:
        try:
            d = json.loads(t)
        except TypeError:
            raise MpSchemaError(f'Type must be string or bytes!')
        if not isinstance(d, dict):
            raise MpSchemaError(f"Deserializing {t} must result in dict!")
        return cls.dict_to_tuple(d)

    @classmethod
    def dict_to_tuple(cls, d: dict) ->  GtElectricMeterCac:
        if "ComponentAttributeClassId" not in d.keys():
            raise MpSchemaError(f"dict {d} missing ComponentAttributeClassId")
        if "MakeModelGtEnumSymbol" not in d.keys():
            raise MpSchemaError(f"dict {d} missing MakeModelGtEnumSymbol")
        d["MakeModel"] = MakeModelMap.gt_to_local(d["MakeModelGtEnumSymbol"])
        if "CommsMethod" not in d.keys():
            d["CommsMethod"] = None
        if "DisplayName" not in d.keys():
            d["DisplayName"] = None

        tuple = GtElectricMeterCac(ComponentAttributeClassId=d["ComponentAttributeClassId"],
                                          CommsMethod=d["CommsMethod"],
                                          MakeModel=d["MakeModel"],
                                          DisplayName=d["DisplayName"],
                                          )
        tuple.check_for_errors()
        return tuple

    @classmethod
    def tuple_to_dc(cls, t: GtElectricMeterCac) -> ElectricMeterCac:
        s = {
            'component_attribute_class_id': t.ComponentAttributeClassId,
            'comms_method': t.CommsMethod,
            'display_name': t.DisplayName,
            'make_model_gt_enum_symbol': MakeModelMap.local_to_gt(t.MakeModel),}
        if s['component_attribute_class_id'] in ElectricMeterCac.by_id.keys():
            dc = ElectricMeterCac.by_id[s['component_attribute_class_id']]
        else:
            dc = ElectricMeterCac(**s)
        return dc

    @classmethod
    def dc_to_tuple(cls, dc: ElectricMeterCac) -> GtElectricMeterCac:
        if dc is None:
            return None
        t = GtElectricMeterCac(ComponentAttributeClassId=dc.component_attribute_class_id,
                                            CommsMethod=dc.comms_method,
                                            MakeModel=dc.make_model,
                                            DisplayName=dc.display_name,
                                            )
        t.check_for_errors()
        return t

    @classmethod
    def type_to_dc(cls, t: str) -> ElectricMeterCac:
        return cls.tuple_to_dc(cls.type_to_tuple(t))

    @classmethod
    def dc_to_type(cls, dc: ElectricMeterCac) -> str:
        return cls.dc_to_tuple(dc).as_type()

    @classmethod
    def dict_to_dc(cls, d: dict) -> ElectricMeterCac:
        return cls.tuple_to_dc(cls.dict_to_tuple(d))
