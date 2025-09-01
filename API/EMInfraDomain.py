import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from json import dumps
from typing import Optional

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

class GeometryNiveau(Enum):
    MIN_1 = 'MIN_1'
    NUL = 'NUL'
    PLUS_1 = 'PLUS_1'

class GeometryBron(Enum):
    MANUEEL = 'MANUEEL'
    MEETTOESTEL = 'MEETTOESTEL'
    OVERERVING = 'OVERERVING'

class GeometryNauwkeurigheid(Enum):
    _5 = '_5'
    _10 = '_10'
    _20 = '_20'
    _30 = '_30'
    _50 = '_50'
    _100 = '_100'
    _200 = '_200'

class KenmerkTypeEnum(Enum):
    HEEFTBIJLAGEBRON = 'HeeftBijlageBron'
    HEEFTTOEGANGSPROCEDUREBRON = 'HeeftToegangsprocedureBron'
    HEEFTAANVULLENDEGEOMETRIEBRON = 'HeeftAanvullendeGeometrieBron'
    GEOMETRIE = 'Geometrie'
    AGENTS = 'Agents'
    SLUITAANOPDOEL = 'SluitAanOpDoel'
    SLUITAANOPBRON = 'SluitAanOpBron'
    LIGTOPDOEL = 'LigtOpDoel'
    LIGTOPBRON = 'LigtOpBron'
    EIGENSCHAPPEN = 'Eigenschappen'
    HOORTBIJ = 'HoortBij'
    BESTEK = 'Bestek'
    LOCATIE = 'Locatie'
    BEVESTIGD_AAN = 'Bevestigd aan'
    GEEFT_BEVESTIGING_AAN = 'Geeft bevestiging aan'
    GEVOED_DOOR = 'Gevoed door'
    GEEFT_VOEDING_AAN = 'Geeft voeding aan'
    AANGESTUURD_DOOR = 'Aangestuurd door'
    GEEFT_STURING_AAN = 'Geeft sturing aan'

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
class Link(BaseDataclass):
    rel: str
    href: str


@dataclass
class ResourceRefDTO(BaseDataclass):
    uuid: str
    links: Optional[list[Link]] = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class DTOList(BaseDataclass):
    links: [Link]
    _from: int
    totalCount: int
    size: int
    _next: str
    previous: str
    data: list


@dataclass
class AssettypeDTO(BaseDataclass):
    _type: str
    links: [Link]
    uuid: str
    createdOn: str
    modifiedOn: str
    uri: str
    korteUri: str
    naam: str
    actief: bool
    definitie: str
    afkorting: str | None = None
    label: str | None = None
    data: dict | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class AssettypeDTOList(DTOList):
    data: list[AssettypeDTO]

    def __post_init__(self):
        self._fix_nested_list_classes({('data', AssettypeDTO)})


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

class ApplicationEnum(Enum):
    EM_INFRA = 'eminfra'
    ELISA_INFRA = 'elisainfra'


class BestekKoppelingStatusEnum(Enum):
    ACTIEF = 'ACTIEF'
    INACTIEF = 'INACTIEF'
    TOEKOMSTIG = 'TOEKOMSTIG'


class BoomstructuurAssetTypeEnum(Enum):
    ASSET = 'asset'
    BEHEEROBJECT = 'beheerobject'

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
class BestekRef(BaseDataclass):
    uuid: str
    type: str
    actief: bool
    links: [Link]
    createdOn: str | None = None
    modifiedOn: str | None = None
    awvId: str | None = None
    eDeltaDossiernummer: str | None = None
    eDeltaBesteknummer: str | None = None
    aannemerNaam: str | None = None
    aannemerReferentie: str | None = None
    nummer: str | None = None
    lot: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


class BestekCategorieEnum(Enum):
    WERKBESTEK = 'WERKBESTEK'
    AANLEVERBESTEK = 'AANLEVERBESTEK'


class SubCategorieEnum(Enum):
    ONDERHOUD = 'ONDERHOUD'
    INVESTERING = 'INVESTERING'
    ONDERHOUD_EN_INVESTERING = 'ONDERHOUD_EN_INVESTERING'


@dataclass
class BestekKoppeling(BaseDataclass):
    startDatum: str
    bestekRef: dict | BestekRef
    status: BestekKoppelingStatusEnum
    eindDatum: str | None = None
    categorie: BestekCategorieEnum | None = None
    subcategorie: SubCategorieEnum | None = None
    bron: str | None = None

    def __post_init__(self):
        self._fix_enums({('categorie', BestekCategorieEnum), ('subcategorie', SubCategorieEnum), ('status', BestekKoppelingStatusEnum)})
        self._fix_nested_classes({('bestekRef', BestekRef)})

@dataclass
class EventType(BaseDataclass):
    description: str
    name: str

@dataclass
class EventContext(BaseDataclass):
    uuid: str
    omschrijving: str
    links: [Link]

    def __post_init__(self):
        self._fix_nested_classes({('links', Link)})

@dataclass
class Event(BaseDataclass):
    type: dict | EventType
    eventNumber: int
    createdOn: str
    determinedOn: str
    data: dict
    links: [Link]

    def __post_init__(self):
        self._fix_nested_classes({('type', EventType), ('links', Link)})

@dataclass
class LocatieKenmerk(BaseDataclass):
    _type: str
    type: dict
    links: [Link]
    locatie: dict | None = None
    geometrie: str | None = None
    omschrijving: str | None = None
    relatie: dict | None = None

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link)})


@dataclass
class GeometryLog(BaseDataclass):
    bron: GeometryBron
    links: [Link]
    nauwkeurigheid: GeometryNauwkeurigheid
    niveau: GeometryNiveau
    uuid: str
    wkt: str

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link)})


@dataclass
class GeometrieKenmerk(BaseDataclass):
    _type: str
    type: dict
    links: [Link]
    logs: list[GeometryLog] | None = None

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link), ('logs', GeometryLog)})

@dataclass
class ToezichterKenmerk(BaseDataclass):
    _type: str
    type: dict
    links: [Link]
    toezichter: dict | None = None
    toezichtGroep: dict | None = None

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link)})

@dataclass
class SchadebeheerderKenmerk(BaseDataclass):
    _type: str
    uuid: str
    createdOn: str
    modifiedOn: str
    naam: str
    referentie: str
    actiefInterval: dict
    contactFiche: dict
    afdeling: dict | None = None
    districtDiensten: dict | None = None
    code: str | None = None
    aanspreking: str | None = None
    links: list[Link] | None = None

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link)})

@dataclass
class IdentiteitKenmerk(BaseDataclass):
    _type: str
    uuid: str
    actief: bool
    systeem: bool
    naam: str
    gebruikersnaam: str
    voornaam: str
    account: dict
    contactFiche: dict
    voId: str | None = None
    bron: str | None = None
    gebruikersrechtOrganisaties: [str] = None
    ldapId: str | None = None
    functie: str | None = None
    links: list[Link] | None = None

    def __post_init__(self):
         self._fix_nested_list_classes({('links', Link)})

@dataclass
class Generator(BaseDataclass):
    uri: str
    version: str
    text: str | None = None


@dataclass
class EntryObjectContent(BaseDataclass):
    value: dict


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

class AssetDTOToestand(Enum):
    IN_ONTWERP = 'IN_ONTWERP'
    GEPLAND = 'GEPLAND'
    GEANNULEERD = 'GEANNULEERD'
    IN_OPBOUW = 'IN_OPBOUW'
    IN_GEBRUIK = 'IN_GEBRUIK'
    VERWIJDERD = 'VERWIJDERD'
    OVERGEDRAGEN = 'OVERGEDRAGEN'
    UIT_GEBRUIK = 'UIT_GEBRUIK'

class ObjectType(Enum):
    INSTALLATIE = 'INSTALLATIE'
    ONDERDEEL = 'ONDERDEEL'
    BEHEEROBJECT = 'BEHEEROBJECT'
    EIGENSCHAP = 'EIGENSCHAP'
    KENMERKTYPE = 'KENMERKTYPE'

@dataclass
class InfraObjectDTO(BaseDataclass):
    _type: str
    uuid: str
    createdOn: str
    modifiedOn: str
    naam: str
    actief: bool
    links: [Link]
    kenmerken: list[dict] | None = None
    toestand: str | None = None
    authorizationMetadata: str | None = None
    parent: list[dict] | None = None
    commentaar: str | None = None
    type: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class AssetDTO(BaseDataclass):
    links: [Link]
    _type: str
    uuid: str
    createdOn: str
    modifiedOn: str
    actief: bool
    toestand: AssetDTOToestand | None = None
    parent: InfraObjectDTO | None = None
    naam: str | None = None
    commentaar: str | None = None
    type: AssettypeDTO | None = None
    kenmerken: list[dict] | None = None # TODO
    authorizationMetadata: list[dict] | None = None # TODO
    children: list[dict] | None = None

    def __post_init__(self):
        self._fix_nested_classes({('type', AssettypeDTO)})
        self._fix_nested_classes({('parent', InfraObjectDTO)})
        self._fix_enums({('toestand', AssetDTOToestand)})
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class BeheerobjectDTO(BaseDataclass):
    _type: str
    uuid: str
    createdOn: str
    modifiedOn: str
    actief: bool
    links: [Link]
    naam: str | None = None
    type: dict | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class BeheerobjectTypeDTO(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    naam: str
    afkorting: str
    actief: bool
    definitie: str
    links: [Link]

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

class DocumentCategorieEnum(Enum):
    AANGEBODEN_SERVICES = 'AANGEBODEN_SERVICES'
    ANDER = 'ANDER'
    ASBUILT_DOSSIER = 'ASBUILT_DOSSIER'
    BEREKENINGSNOTA = 'BEREKENINGSNOTA'
    BRIEF = 'BRIEF'
    CONFIGBESTAND = 'CONFIGBESTAND'
    CONSTRUCTIE_EN_MONTAGEPLAN = 'CONSTRUCTIE_EN_MONTAGEPLAN'
    CONTROLEMETING_EBS = 'CONTROLEMETING_EBS'
    DIMCONFIGURATIE = 'DIMCONFIGURATIE'
    ELEKTRISCH_SCHEMA = 'ELEKTRISCH_SCHEMA'
    FACTUUR = 'FACTUUR'
    FOTO = 'FOTO'
    HANDLEIDING = 'HANDLEIDING'
    INTERVENTIEVERSLAG = 'INTERVENTIEVERSLAG'
    KABELAANSLUITSCHEMA = 'KABELAANSLUITSCHEMA'
    KEURINGSVERSLAG = 'KEURINGSVERSLAG'
    LICHTSTUDIE = 'LICHTSTUDIE'
    LUSSENMEETRAPPORT = 'LUSSENMEETRAPPORT'
    MEETRAPPORT = 'MEETRAPPORT'
    M_PLAN = 'M_PLAN'
    OFFERTE = 'OFFERTE'
    OPROEPDOCUMENT = 'OPROEPDOCUMENT'
    PV_INGEBREKESTELLING = 'PV_INGEBREKESTELLING'
    PV_OPLEVERING = 'PV_OPLEVERING'
    PV_SCHADEVERWEKKER = 'PV_SCHADEVERWEKKER'
    RISICOANALYSE = 'RISICOANALYSE'
    SOFTWARE_DEPENDENCIES = 'SOFTWARE_DEPENDENCIES'
    TECHNISCHE_FICHE = 'TECHNISCHE_FICHE'
    TRACO_ATTEST = 'TRACO_ATTEST'
    V_PLAN = 'V_PLAN'

class ProvincieEnum(Enum):
    ANTWERPEN = 'antwerpen'
    WEST_VLAANDEREN = 'west-vlaanderen'
    OOST_VLAANDEREN = 'oost-vlaanderen'
    VLAAMS_BRABANT = 'vlaams-brabant'
    LIMBURG = 'limburg'
    BRUSSEL = 'brussel'

class ToezichtgroepTypeEnum(Enum):
    INTERN = 'INTERN'
    EXTERN = 'EXTERN'

@dataclass
class ResourceRefDTO(BaseDataclass):
    uuid: str
    links: Optional[list[Link]] = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class AssetDocumentDTO(BaseDataclass):
    uuid: str
    categorie: DocumentCategorieEnum
    naam: str
    document: [ResourceRefDTO]
    links: [Link]
    omschrijving: str | None = None

    def __hash__(self):
        # Hash based on name and value
        return hash(self.uuid)

    def __post_init__(self):
        self._fix_enums({('categorie', DocumentCategorieEnum)})
        self._fix_nested_list_classes({('links', Link)})
        self._fix_nested_list_classes({('document', ResourceRefDTO)})

        
@dataclass
class BetrokkenerelatieDTO(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    bron: dict # TODO wijzigen naar Object
    doel: dict # TODO wijzigen naar Object
    rol: str # TODO enum
    links: [Link]

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class RelatieTypeDTO(BaseDataclass):
    _type: str
    uuid: str
    createdOn: str
    modifiedOn: str
    actief: bool
    type: dict
    toestand: AssetDTOToestand
    links: [Link]
    naam: str | None = None
    authorizationMetadata: dict | None = None
    commentaar: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class RelatieTypeDTOList(BaseDataclass):
    relatieType: RelatieTypeDTO | dict
    links: [Link]

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})
        self._fix_nested_classes({('relatieType', RelatieTypeDTO)})

@dataclass
class PostitDTO(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    links: [Link]
    startDatum: str
    eindDatum: str  # mandatory
    commentaar: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})


@dataclass
class AgentDTO(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    naam: str
    actief: bool
    links: [Link]
    contactInfo: [dict] = None
    voId: str | None = None
    ovoCode: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})



@dataclass
class KenmerkTypeDTO(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    naam: str
    actief: bool
    predefined: bool
    standard: bool
    definitie: str
    links: [Link]

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class KenmerkType(BaseDataclass):
    _type: str
    type: KenmerkTypeDTO
    links: [Link]
    types: dict | None = None
    bestekRef: dict | None = None
    bestekKoppelingen: dict | None = None
    toezichter: dict | None =  None
    toezichtGroep: dict | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class ToezichtgroepDTO(BaseDataclass):
    _type: str
    naam: str
    uuid: str
    referentie: str
    actiefInterval: str
    contactFiche: dict
    links: [Link]
    omschrijving: str | None = None
    createdOn: str | None = None
    modifiedOn: str | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class Eigenschap(BaseDataclass):
    uuid: str
    createdOn: str
    modifiedOn: str
    uri: str
    label: str
    naam: str
    alleenLezen: bool
    actief: bool
    definitie: str
    categorie: str
    type: dict
    links: [Link]
    kardinaliteitMin: int | None = None
    kardinaliteitMax: int | None = None

    def __post_init__(self):
        self._fix_nested_list_classes({('links', Link)})

@dataclass
class EigenschapValueDTO(BaseDataclass):
    typedValue: dict
    determinedOn: str
    determinedBy: str
    eigenschap: Eigenschap
    actief: bool
    kenmerkType: KenmerkTypeDTO
    alias: str | None = None

    def __post_init__(self):
        self._fix_nested_classes({('eigenschap', Eigenschap), ('kenmerkType', KenmerkTypeDTO)})

@dataclass
class EigenschapValueUpdateDTO(BaseDataclass):
    typedValue: dict
    eigenschap: Eigenschap

    def __post_init__(self):
        self._fix_nested_classes({('eigenschap', Eigenschap)})

@dataclass
class AssetTypeKenmerkTypeAddDTO(BaseDataclass):
    kenmerkType: ResourceRefDTO

    def __post_init__(self):
        self._fix_nested_classes({('kenmerkType', KenmerkTypeDTO)})


@dataclass
class AssetTypeKenmerkTypeDTO(BaseDataclass):
    kenmerkType: KenmerkTypeDTO
    actief: bool
    standard: bool

    def __post_init__(self):
        self._fix_nested_classes({('kenmerkType', KenmerkTypeDTO)})


def construct_naampad(asset: AssetDTO) -> str:
    naampad = asset.naam
    parent = asset.parent
    while parent is not None:
        # parent is dictionary (Beheerobject)
        if isinstance(parent, dict):
            naampad = parent.get("naam") + '/' + naampad
            parent = parent.get("parent")

        else:
            naampad = f'{parent.naam}/{naampad}'
            parent = parent.parent
    return naampad

