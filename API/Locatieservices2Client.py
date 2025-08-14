import logging

from pathlib import Path

from API.Enums import AuthType, Environment
from API.Locatieservices2Domain import WegsegmentPuntLocatie
from API.RequesterFactory import RequesterFactory


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

        return WegsegmentPuntLocatie.from_dict(response.json())

    def zoek_puntlocatie_via_wegsegment(self, ident8: str, opschrift: float, afstand: float = 0.0) -> WegsegmentPuntLocatie:
        response = self.requester.get(
            url=f'rest/puntlocatie/op/weg/{ident8}/via/opschrift?opschrift={opschrift}&afstand={afstand}'
        )
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return WegsegmentPuntLocatie.from_dict(response.json())