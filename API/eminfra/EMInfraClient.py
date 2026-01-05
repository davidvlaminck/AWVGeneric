from pathlib import Path

from API.eminfra.AgentService import AgentService
from API.eminfra.AssetService import AssetService
from API.eminfra.AssettypeService import AssettypeService
from API.eminfra.BeheerobjectService import BeheerobjectService
from API.eminfra.BestekService import BestekService
from API.eminfra.DocumentService import DocumentService
from API.eminfra.EigenschapService import EigenschapService
from API.eminfra.EventService import EventService
from API.eminfra.FeedService import FeedService
from API.eminfra.GeometrieService import GeometrieService
from API.eminfra.GraphService import GraphService
from API.eminfra.KenmerkService import KenmerkService
from API.eminfra.LocatieService import LocatieService
from API.eminfra.OnderdeelService import OnderdeelService
from API.eminfra.PostitService import PostitService
from API.eminfra.RelatieService import RelatieService
from API.eminfra.SchadebeheerderService import SchadebeheerderService
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
        self.assettypes = AssettypeService(self.requester)
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