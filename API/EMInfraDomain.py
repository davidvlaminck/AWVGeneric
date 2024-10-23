import dataclasses
from dataclasses import dataclass
from enum import Enum
from json import dumps

import dataclasses
from types import UnionType
from typing import Union, get_origin, get_args, Self

# If dataclass has __dict_factory_override__, use that instead of dict_factory
_asdict_inner_actual = dataclasses._asdict_inner
def _asdict_inner(obj, dict_factory):

    # if override exists, intercept and return that instead
    if dataclasses._is_dataclass_instance(obj):
        if getattr(obj, '__dict_factory_override__', None):
            user_dict = obj.__dict_factory_override__()

            for k, v in user_dict.items(): # in case of further nesting
                if dataclasses._is_dataclass_instance(v):
                    user_dict[k] = _asdict_inner(v, dict_factory)
            return user_dict

    # otherwise do original behavior
    return _asdict_inner_actual(obj, dict_factory)
dataclasses._asdict_inner = _asdict_inner
asdict = dataclasses.asdict


class OperatorEnum(Enum):
    EQ = 'EQ'
    CONTAINS = 'CONTAINS'
    GT = 'GT'
    GTE = 'GTE'
    LT = 'LT'
    LTE = 'LTE'
    IN = 'IN'
    STARTS_WITH = 'STARTS_WITH'
    INTERSECTS = 'INTERSECTS'


class LogicalOpEnum(Enum):
    AND = 'AND'
    OR = 'OR'


@dataclass
class BaseDataclass:
    reserved_word_list = ('from_')

    def __dict_factory_override__(self):
        normal_dict = {k: getattr(self, k) for k in self.__dataclass_fields__}
        d = {}
        for k, v in normal_dict.items():
            if k in self.reserved_word_list:
                k = k[:-1]

            d[k] = v.value if isinstance(v, Enum) else v
        return d

    def asdict(self):
        return asdict(self)

    def json(self):
        """
        get the json formated string
        """
        return dumps(self.asdict())

    @classmethod
    def from_dict(cls, dict_: dict) -> Self:
        for k in list(dict_.keys()):
            if k in cls.reserved_word_list:
                dict_[f'{k}_'] = dict_[k]
                del dict_[k]
        return cls(**dict_)

    # needs enum fix
    # needs to call from_dict for nested classes

    # def __post_init__(self):
    #     f = dataclasses.fields(self)
    #     for field in f:
    #         for t in get_args(field.type):
    #             if issubclass(t, BaseDataclass):
    #                 print(field.name)
    #                 attribute_value = getattr(self, field.name)
    #                 if attribute_value is not None and isinstance(attribute_value, dict):
    #                     new_value = t(**attribute_value)
    #                     setattr(self, field.name, t(**attribute_value))
    #                 pass
    #             #setattr(self, field.name, o(**getattr(self, field.name)))
    #         if get_origin(field.type) is UnionType:
    #             print('UnionType')
    #
    #         if field.name in self.reserved_word_list:
    #             setattr(self, field.name[:-1], getattr(self, field.name))
    #             delattr(self, field.name)




@dataclass
class TermDTO(BaseDataclass):
    property: str
    value: object
    operator: OperatorEnum
    logicalOp: LogicalOpEnum | None = None
    negate: bool | None = False


@dataclass
class ExpressionDTO(BaseDataclass):
    terms: list[dict] | list[TermDTO]
    logicalOp: LogicalOpEnum | None = None

    def __post_init__(self):
        if self.terms is not None and isinstance(self.terms, list) and len(self.terms) > 0 and isinstance(self.terms[0], dict):
            self.terms = [TermDTO(**t) for t in self.terms]

    def to_dict(self):
        dict_ = {}
        for k, v in self.__dataclass_fields__.items():
            if getattr(self, k) is not None:
                value = getattr(self, k)
                if isinstance(value, Enum):
                    dict_[k] = value.value
                elif dataclasses.is_dataclass(value):
                    dict_[k] = value.to_dict()
                elif isinstance(value, list):
                    if dataclasses.is_dataclass(value[0]):
                        dict_[k] = [v.to_dict() for v in value]
                    else:
                        dict_[k] = value
                else:
                    dict_[k] = value
        return dict_


@dataclass
class SelectionDTO:
    expressions: list[dict] | list[ExpressionDTO]
    settings: dict | None = None

    def __post_init__(self):
        if self.expressions is not None and isinstance(self.expressions, dict)  and len(self.expressions) > 0 and isinstance(self.expressions[0], dict):
            self.expressions = [ExpressionDTO(**e) for e in self.expressions]

    def to_dict(self):
        dict_ = {}
        for k, v in self.__dataclass_fields__.items():
            if getattr(self, k) is not None:
                value = getattr(self, k)
                if isinstance(value, Enum):
                    dict_[k] = value.value
                elif dataclasses.is_dataclass(value):
                    dict_[k] = value.to_dict()
                elif isinstance(value, list):
                    if dataclasses.is_dataclass(value[0]):
                        dict_[k] = [v.to_dict() for v in value]
                    else:
                        dict_[k] = value
                else:
                    dict_[k] = value
        return dict_


@dataclass
class ExpansionsDTO:
    fields: [str]


class PagingModeEnum(Enum):
    OFFSET = 'OFFSET'
    CURSOR = 'CURSOR'


class DirectionEnum(Enum):
    ASC = 'ASC'
    DESC = 'DESC'


@dataclass
class QueryDTO:
    size: int
    from_: int
    selection: dict | SelectionDTO | None = None
    fromCursor: str | None = None
    orderByProperty: str | None = None
    settings: dict | None = None
    expansions: dict | ExpansionsDTO | None = None
    orderByDirection: DirectionEnum | None = None
    pagingMode: PagingModeEnum | None = None

    def __post_init__(self):
        if self.selection is not None and isinstance(self.selection, dict):
            self.selection = SelectionDTO(**self.selection)
        if self.expansions is not None and isinstance(self.expansions, dict):
            self.expansions = ExpansionsDTO(**self.expansions)
        if self.pagingMode is not None:
            self.pagingMode = PagingModeEnum(self.pagingMode)

    def to_dict(self):
        dict_ = {}
        for k, v in self.__dataclass_fields__.items():
            if getattr(self, k) is not None:
                value = getattr(self, k)
                if k == 'from_':
                    k = 'from'
                if isinstance(value, Enum):
                    dict_[k] = value.value
                elif dataclasses.is_dataclass(value):
                    dict_[k] = value.to_dict()
                elif isinstance(value, list):
                    if dataclasses.is_dataclass(value[0]):
                        dict_[k] = [v.to_dict() for v in value]
                    else:
                        dict_[k] = value
                else:
                    dict_[k] = value
        return dict_


@dataclass
class Link:
    rel: str
    href: str


@dataclass
class BestekRef:
    uuid: str
    awvId: str
    eDeltaDossiernummer: str
    eDeltaBesteknummer: str
    type: str
    aannemerNaam: str
    aannemerReferentie: str
    actief: bool
    links: [Link]
    nummer: str | None = None
    lot: str | None = None

    def __post_init__(self):
        self.links = [Link(**l) for l in self.links]


class CategorieEnum(Enum):
    WERKBESTEK = 'WERKBESTEK'
    AANLEVERBESTEK = 'AANLEVERBESTEK'


class SubCategorieEnum(Enum):
    ONDERHOUD = 'ONDERHOUD'
    INVESTERING = 'INVESTERING'
    ONDERHOUD_EN_INVESTERING = 'ONDERHOUD_EN_INVESTERING'


@dataclass
class BestekKoppeling:
    startDatum: str
    eindDatum: str
    bestekRef: dict | BestekRef
    status: str
    categorie: CategorieEnum | None = None
    subcategorie: SubCategorieEnum | None = None
    bron: str | None = None

    def __post_init__(self):
        if self.bestekRef is not None and isinstance(self.bestekRef, dict):
            self.bestekRef = BestekRef(**self.bestekRef)
        if self.categorie is not None:
            self.categorie = CategorieEnum(self.categorie)
        if self.subcategorie is not None:
            self.subcategorie = SubCategorieEnum(self.subcategorie)
