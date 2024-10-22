import dataclasses
import json
from dataclasses import dataclass, asdict
from enum import Enum

from API.AbstractRequester import AbstractRequester


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
class TermDTO:
    property: str
    value: object
    operator: OperatorEnum
    logicalOp: LogicalOpEnum | None = None
    negate: bool | None = False

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
class ExpressionDTO:
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

class EMInfraClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'eminfra/'

    def get_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str) -> [BestekKoppeling]:
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        print(response.json()['data'])

        return [BestekKoppeling(**item) for item in response.json()['data']]

    def get_bestekref_by_eDelta_dossiernummer(self, eDelta_dossiernummer: str) -> [BestekRef]:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaDossiernummer', value=eDelta_dossiernummer,
                                                    operator=OperatorEnum.EQ)])]))

        response = self.requester.post('core/api/bestekrefs/search', json=query_dto.to_dict())
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        print(response.json()['data'])

        return [BestekRef(**item) for item in response.json()['data']]


