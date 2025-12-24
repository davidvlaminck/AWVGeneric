import json
import logging

from shapely import wkt
from shapely.errors import WKTReadingError
from API.EMInfraDomain import GeometrieKenmerk, GeometryNiveau, GeometryBron, GeometryNauwkeurigheid
from API.eminfra.wkt_validator import is_valid_wkt


class GeometrieService:
    def __init__(self, requester):
        self.requester = requester
        self.GEOMETRIE_UUID = 'aabe29e0-9303-45f1-839e-159d70ec2859'

    def get_geometrie(self, asset_uuid: str) -> GeometrieKenmerk:
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.GEOMETRIE_UUID}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return GeometrieKenmerk.from_dict(response.json())

    def delete_geometrie(self, asset_uuid: str, log_id: str):
        response = self.requester.delete(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.GEOMETRIE_UUID}/logs/{log_id}')
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def add_geometrie(self, asset_uuid, wkt_geometry, geometry_log: GeometryNiveau = GeometryNiveau.MIN_1
                      , geometry_bron: GeometryBron = GeometryBron.MANUEEL
                      , geometry_nauwkeurigheid: GeometryNauwkeurigheid = GeometryNauwkeurigheid._50):
        """
        :param asset_uuid:
        :param wkt_geometry: Well Known Text geometrie
        :param geometry_log: default waarde "-1"
        :param geometry_bron: default waarde "Manueel"
        :param geometry_nauwkeurigheid: default waarde "<50 cm"
        :return:
        """
        if not is_valid_wkt(wkt_string=wkt_geometry):
            raise ValueError(f'WKT Geometry is invalid: {wkt_geometry}.')
        json_body = {
            "wkt": f"{wkt_geometry}",
            "niveau": f"{geometry_log.value}",
            "nauwkeurigheid": f"{geometry_nauwkeurigheid.value}",
            "bron": f"{geometry_bron.value}"
        }
        response = self.requester.post(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.GEOMETRIE_UUID}/logs'
            , data=json.dumps(json_body)
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_geometrie(self, asset_uuid: str, wkt_geometry: str) -> None:
        """
        Update de bestaande geometrie eigenschap door 3 hulpfuncties in te roepen:
        - get_geometrie()
        - delete_geometrie()
        - add_geometrie()

        :param asset_uuid:
        :param wkt_geometry:
        :return:
        """
        if not is_valid_wkt(wkt_string=wkt_geometry):
            raise ValueError(f'WKT Geometry is invalid: {wkt_geometry}.')

        # step 1: search existing geometry
        geometriekenmerk = self.get_geometrie(asset_uuid=asset_uuid)

        # step 2: remove existing geometry
        if geometriekenmerk.logs:
            if log_id := geometriekenmerk.logs[0].uuid:
                self.delete_geometrie(asset_uuid, log_id)

        # step 3: add new geometry
        self.add_geometrie(asset_uuid=asset_uuid, wkt_geometry=wkt_geometry)