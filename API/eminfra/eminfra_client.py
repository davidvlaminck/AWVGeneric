from pathlib import Path

from API.eminfra.agents import AgentService
from API.eminfra.assets import AssetService
from API.eminfra.assettypes import AssettypesService
from API.eminfra.beheerobject import BeheerobjectService
from API.eminfra.bestekken import BestekService
from API.eminfra.documenten import DocumentService
from API.eminfra.eigenschappen import EigenschapService
from API.eminfra.events import EventService
from API.eminfra.feed import FeedService
from API.eminfra.geometrie import GeometrieService
from API.eminfra.graph import GraphService
from API.eminfra.kenmerken import KenmerkService
from API.eminfra.locatie import LocatieService
from API.eminfra.onderdeel import OnderdeelService
from API.eminfra.postits import PostitService
from API.eminfra.relaties import RelatieService
from API.eminfra.schadebeheerder import SchadebeheerderService
from API.eminfra.toezichter import ToezichterService

from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

        # Sub-services
        self.agents = AgentService(self.requester)
        self.assets = AssetService(self.requester)
        self.assettypes = AssettypesService(self.requester)
        self.beheerobject = BeheerobjectService(self.requester)
        self.bestekken = BestekService(self.requester)
        self.documenten = DocumentService(self.requester)
        self.eigenschappen = EigenschapService(self.requester)
        self.events = EventService(self.requester)
        self.feed = FeedService(self.requester)
        self.geometrie = GeometrieService(self.requester)
        self.graph = GraphService(self.requester)
        self.kenmerken = KenmerkService(self.requester)
        self.locatie = LocatieService(self.requester)
        self.onderdeel = OnderdeelService(self.requester)
        self.postit = PostitService(self.requester)
        self.relatie = RelatieService(self.requester)
        self.schadebeheerder = SchadebeheerderService(self.requester)
        self.toezichter = ToezichterService(self.requester)

    def get_oef_schema_as_json(self, name: str) -> str:
        url = f"core/api/otl/schema/oef/{name}"
        content = self.requester.get(url).content
        return content.decode("utf-8")