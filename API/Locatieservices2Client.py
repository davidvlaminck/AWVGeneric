import json
import logging
import uuid

from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta

from pathlib import Path
from datetime import datetime

from API.EMInfraDomain import OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssettypeDTOList, DTOList, AssetDTO, BetrokkenerelatieDTO, AgentDTO, \
    PostitDTO, LogicalOpEnum, BestekCategorieEnum, BestekKoppelingStatusEnum, AssetDocumentDTO, LocatieKenmerk, \
    LogicalOpEnum, ToezichterKenmerk, IdentiteitKenmerk, AssetTypeKenmerkTypeDTO, KenmerkTypeDTO, \
    AssetTypeKenmerkTypeAddDTO, ResourceRefDTO, Eigenschap, Event, EventType, ObjectType, EventContext, ExpansionsDTO, \
    RelatieTypeDTO, KenmerkType, EigenschapValueDTO, RelatieTypeDTOList, BeheerobjectDTO, ToezichtgroepTypeEnum, \
    ToezichtgroepDTO, BaseDataclass, BeheerobjectTypeDTO, BoomstructuurAssetTypeEnum, KenmerkTypeEnum, AssetDTOToestand, \
    EigenschapValueUpdateDTO, GeometryNiveau, GeometryBron, GeometryNauwkeurigheid, GeometrieKenmerk
from API.Enums import AuthType, Environment
from API.Locatieservices2Domain import WegsegmentPuntLocatie
from API.RequesterFactory import RequesterFactory
from utils.date_helpers import validate_dates, format_datetime
from utils.query_dto_helpers import add_expression
import os


@dataclass
class Query(BaseDataclass):
    size: int
    filters: dict
    orderByProperty: str
    fromCursor: str | None = None


class Locatieservices2Client:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'locatieservices2/'

    def zoek_puntlocatie_via_xy(self, x: float, y: float, zoekafstand: int = 50) -> WegsegmentPuntLocatie:
        """
        Zoek de dichtstbijgelegen puntlocatie
        :param x: x coordinate
        :param y: y coordinate
        :param zoekafstand: zoekafstand in meter. Default 50.
        :return:
        """
        response = self.requester.get(
            url=f'rest/puntlocatie/via/xy?/zoekafstand={zoekafstand}&x={x}&y={y}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [WegsegmentPuntLocatie.from_dict(item) for item in response.json()['data']]