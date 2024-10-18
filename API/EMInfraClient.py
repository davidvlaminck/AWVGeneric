from dataclasses import dataclass
from enum import Enum

from API.AbstractRequester import AbstractRequester


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

