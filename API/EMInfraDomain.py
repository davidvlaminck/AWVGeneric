import dataclasses
from dataclasses import dataclass
from enum import Enum
from json import dumps
from typing import Self

_asdict_inner_actual = dataclasses._asdict_inner
def _asdict_inner(obj, dict_factory):

    # if override exists, intercept and return that instead
    if dataclasses._is_dataclass_instance(obj):
        if getattr(obj, '__dict_factory_override__', None):
            user_dict = obj.__dict_factory_override__()

            for k, v in user_dict.items(): # in case of further nesting
                if isinstance(v, list) and len(v) > 0 and dataclasses._is_dataclass_instance(v[0]):
                    user_dict[k] = [_asdict_inner(vv, dict_factory) for vv in v]
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


RESERVED_WORD_LIST = ('from_')

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
        get the json formated string
        """
        d = self.asdict()
        return dumps(self.asdict())

    @classmethod
    def from_dict(cls, dict_: dict) -> Self:
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
        self._fix_nested_list_classes({('terms', TermDTO)})


@dataclass
class SelectionDTO(BaseDataclass):
    expressions: list[dict] | list[ExpressionDTO]
    settings: dict | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('expressions', ExpressionDTO)})


@dataclass
class ExpansionsDTO(BaseDataclass):
    fields: [str]


class PagingModeEnum(Enum):
    OFFSET = 'OFFSET'
    CURSOR = 'CURSOR'


class DirectionEnum(Enum):
    ASC = 'ASC'
    DESC = 'DESC'


@dataclass
class QueryDTO(BaseDataclass):
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
        self._fix_enums({('pagingMode', PagingModeEnum)})
        self._fix_nested_classes({('selection', SelectionDTO), ('expansions', ExpansionsDTO)})


@dataclass
class Link(BaseDataclass):
    rel: str
    href: str


@dataclass
class BestekRef(BaseDataclass):
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
        self._fix_nested_classes({('links', Link)})


class CategorieEnum(Enum):
    WERKBESTEK = 'WERKBESTEK'
    AANLEVERBESTEK = 'AANLEVERBESTEK'


class SubCategorieEnum(Enum):
    ONDERHOUD = 'ONDERHOUD'
    INVESTERING = 'INVESTERING'
    ONDERHOUD_EN_INVESTERING = 'ONDERHOUD_EN_INVESTERING'


@dataclass
class BestekKoppeling(BaseDataclass):
    startDatum: str
    eindDatum: str
    bestekRef: dict | BestekRef
    status: str
    categorie: CategorieEnum | None = None
    subcategorie: SubCategorieEnum | None = None
    bron: str | None = None

    def __post_init__(self):
        self._fix_enums({('categorie', CategorieEnum), ('subcategorie', SubCategorieEnum)})
        self._fix_nested_classes({('bestekRef', BestekRef)})


@dataclass
class Generator(BaseDataclass):
    uri: str
    version: str
    text: str | None = None


# @dataclass
# class EntryObjectValue(BaseDataclass):
#     event-type: str
#     asset-type: str | None = None
#     uuids: [str] | None = None
#     aim-ids: [str] | None = None
#


@dataclass
class EntryObjectContent(BaseDataclass):
    value: str


@dataclass
class EntryObject(BaseDataclass):
    id: str
    updated: str
    content: EntryObjectContent | dict
    _type: str
    links: list[Link] | None = None

    def __post_init__(self):
        self._fix_nested_classes({('content', EntryObjectContent)})
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class FeedPage(BaseDataclass):
    id: str
    base: str
    title: str
    updated: str
    generator: Generator
    links: list[Link] | None = None
    entries: list[EntryObject] | None = None

    def __post_init__(self):
        self._fix_nested_classes({('generator', Generator)})
        self._fix_nested_list_classes({('links', Link), ('entries', EntryObject)})
