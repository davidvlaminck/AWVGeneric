from dataclasses import dataclass

from API.AbstractRequester import AbstractRequester


@dataclass
class BestekRef:
    def __init__(self, **kwargs):
        self.uuid: str
        self.awvId: str
        self.eDeltaDossiernummer: str
        self.eDeltaBesteknummer: str
        self.type: str
        self.aannemerNaam: str
        self.aannemerReferentie: str
        self.__dict__.update(kwargs)

    def __repr__(self):
        return self.__dict__.__str__()

class BestekKoppeling:
    def __init__(self, **kwargs):
        self.startDatum: str
        self.eindDatum: str
        self.bestekRef: BestekRef
        self.status: str
        self.categorie: str
        self.__dict__.update(kwargs)

    def __repr__(self):
        return self.__dict__.__str__()



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

        return [BestekKoppeling(**item) for item in response.json()['data']]

