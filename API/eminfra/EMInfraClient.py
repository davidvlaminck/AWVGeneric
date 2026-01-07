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
from API.eminfra.ToezichterService import ToezichterService

from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory

class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

        # Sub-services
        self.agent_service = AgentService(self.requester)
        self.asset_service = AssetService(self.requester)
        self.assettype_service = AssettypeService(self.requester)
        self.beheerobject_service = BeheerobjectService(self.requester)
        self.bestek_service = BestekService(self.requester)
        self.document_service = DocumentService(self.requester)
        self.eigenschap_service = EigenschapService(self.requester)
        self.event_service = EventService(self.requester)
        self.feed_service = FeedService(self.requester)
        self.geometrie_service = GeometrieService(self.requester)
        self.graph_service = GraphService(self.requester)
        self.kenmerk_service = KenmerkService(self.requester)
        self.locatie_service = LocatieService(self.requester)
        self.onderdeel_service = OnderdeelService(self.requester)
        self.postit_service = PostitService(self.requester)
        self.relatie_service = RelatieService(self.requester)
        self.schadebeheerder_service = SchadebeheerderService(self.requester)
        self.toezichter_service = ToezichterService(self.requester)

    def get_oef_schema_as_json(self, name: str) -> str:
        url = f"core/api/otl/schema/oef/{name}"
        content = self.requester.get(url).content
        return content.decode("utf-8")