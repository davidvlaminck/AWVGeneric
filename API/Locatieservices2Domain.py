import json
from dataclasses import dataclass
from enum import Enum
from json import dumps


class WegsegmenttypeEnum(Enum):
    WEGSEGMENTPUNTLOCATIE = 'WegsegmentPuntLocatie'
    # aanvullen met andere types

RESERVED_WORD_LIST = ('from_', '_next')

@dataclass
class BaseDataclass:
    def __dict_factory_override__(self):
        normal_dict = {k: getattr(self, k) for k in self.__dataclass_fields__}
        d = {}
        for k, v in normal_dict.items():
            if k in RESERVED_WORD_LIST:
                k = k[:-1]

            d[k] = v.value if isinstance(v, Enum) else v
        return d

    def asdict(self):
        return asdict(self)

    def json(self):
        """
        get the json formatted string
        """
        d = self.asdict()
        return dumps(self.asdict())

    @classmethod
    def from_dict(cls, dict_: dict):
        for k in list(dict_.keys()):
            if k in RESERVED_WORD_LIST:
                dict_[f'{k}_'] = dict_[k]
                del dict_[k]
        return cls(**dict_)

    def _fix_enums(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None:
                setattr(self, field_tuple[0], field_tuple[1](attr))

    def _fix_nested_classes(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None and isinstance(attr, dict):
                setattr(self, field_tuple[0], field_tuple[1].from_dict(attr))

    def _fix_nested_list_classes(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None and isinstance(attr, list) and len(attr) > 0 and isinstance(attr[0], dict):
                setattr(self, field_tuple[0], [field_tuple[1].from_dict(a) for a in attr])


    def __str__(self):
        return json.dumps(self.asdict(), indent=4, sort_keys=True)

@dataclass
class WegsegmentId(BaseDataclass):
    gidn: str
    oidn: str
    uidn: str

@dataclass
class JSONGeom(BaseDataclass):
    type: str
    coordinates: list[float]
    bbox: list[float]
    crs: dict

@dataclass
class WegsegmentPuntLocatie(BaseDataclass):
   type: WegsegmenttypeEnum
   geometry: JSONGeom
   projectie: JSONGeom
   wegsegmentId: WegsegmentId