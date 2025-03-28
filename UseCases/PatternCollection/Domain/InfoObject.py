import abc

directional_relations = {
    "onderdeel#HeeftAanvullendeGeometrie",
    "onderdeel#VoedtAangestuurd",
    "onderdeel#HeeftBeheer",
    "onderdeel#IsInspectieVan",
    "onderdeel#SluitAanOp",
    "onderdeel#Voedt",
    "onderdeel#HeeftNetwerkProtectie",
    "onderdeel#HoortBij",
    "onderdeel#Omhult",
    "onderdeel#IsNetwerkECC",
    "onderdeel#IsAdmOnderdeelVan",
    "onderdeel#HeeftBetrokkene",
    "onderdeel#HeeftNetwerktoegang",
    "onderdeel#HeeftToegangsprocedure",
    "onderdeel#LigtOp",
    "onderdeel#IsSWOnderdeelVan",
    "onderdeel#IsSWGehostOp",
}
non_directional_relations = {
    "onderdeel#Bevestiging",
    "onderdeel#Sturing"
}


def is_relation(short_type: str) -> bool:
    return short_type in directional_relations or short_type in non_directional_relations


def is_directional_relation(short_type: str) -> bool:
    return short_type in directional_relations


def full_uri_to_short_type(uri: str) -> str:
    short_uri = uri.split('/')[-1]
    return f'lgc:{short_uri}' if uri.startswith('https://lgc') else short_uri


class InfoObject(abc.ABC):
    @abc.abstractmethod
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, active: bool = True):
        self.uuid: str = uuid
        self.short_type: str = short_type
        self.active: bool = active
        self.attr_dict: dict = attr_dict
        self.is_relation: bool
        self.is_directional_relation: bool

    def __str__(self):
        return f'{self.short_type} {self.uuid}\n{self.attr_dict}'


class NodeInfoObject(InfoObject):
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, active: bool = True):
        super().__init__(uuid, short_type, attr_dict, active)
        self.relations: dict = {}
        self.is_relation: bool = False
        self.is_directional_relation: bool = False


class RelationInfoObject(InfoObject):
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, bron: NodeInfoObject, doel: NodeInfoObject,
                 active: bool = True):
        super().__init__(uuid, short_type, attr_dict, active)
        self.is_relation: bool = True
        self.is_directional_relation: bool = (self.is_relation and short_type not in non_directional_relations)
        self.bron: NodeInfoObject = bron
        self.doel: NodeInfoObject = doel
