import json
import logging

from API.eminfra.EMInfraDomain import GeometrieKenmerk, GeometryNiveau, GeometryBron, GeometryNauwkeurigheid, AssetDTO
from API.eminfra.wkt_validator import is_valid_wkt


class GeometrieService:
    def __init__(self, requester):
        self.requester = requester
        self.GEOMETRIE_UUID = 'aabe29e0-9303-45f1-839e-159d70ec2859'

    def get_geometrie_by_uuid(self, asset_uuid: str) -> GeometrieKenmerk:
        """
        Ophalen van het GeometrieKenmerk van een asset

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :return: GeometrieKenmerk
        :rtype:
        """
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.GEOMETRIE_UUID}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return GeometrieKenmerk.from_dict(response.json())

    def get_geometrie(self, asset: AssetDTO) -> GeometrieKenmerk:
        """
        Ophalen van het GeometrieKenmerk van een asset

        :param asset: Asset
        :type asset: AssetDTO
        :return: GeometrieKenmerk
        :rtype:
        """
        return self.get_geometrie_by_uuid(asset_uuid=asset.uuid)

    def delete_geometrie_by_uuid(self, asset_uuid: str, log_id: str) -> None:
        """
        Verwijderen/wissen van het GeometrieKenmerk van een asset.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param log_id: Level Of Geometrie ID
        :type log_id: str
        :return: None
        :rtype:
        """
        response = self.requester.delete(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.GEOMETRIE_UUID}/logs/{log_id}')
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def delete_geometrie(self, asset: AssetDTO, log_id: str) -> None:
        """
        Verwijderen/wissen van het GeometrieKenmerk van een asset.

        :param asset:
        :type asset: AssetDTO
        :param log_id: Level Of Geometrie ID
        :type log_id: str
        :return: None
        :rtype:
        """
        return self.delete_geometrie_by_uuid(asset_uuid=asset.uuid, log_id=log_id)

    def add_geometrie_by_uuid(self, asset_uuid: str, wkt_geometry: str, geometry_log: GeometryNiveau = GeometryNiveau.MIN_1
                      , geometry_bron: GeometryBron = GeometryBron.MANUEEL
                      , geometry_nauwkeurigheid: GeometryNauwkeurigheid = GeometryNauwkeurigheid._50) -> None:
        """
        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param wkt_geometry: Well Known Text geometrie
        :type wkt_geometry: str
        :param geometry_log: default waarde "-1"
        :param geometry_bron: default waarde "Manueel"
        :param geometry_nauwkeurigheid: default waarde "<50 cm"
        :return: None
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

    def add_geometrie(self, asset: AssetDTO, wkt_geometry: str, geometry_log: GeometryNiveau = GeometryNiveau.MIN_1
                      , geometry_bron: GeometryBron = GeometryBron.MANUEEL
                      , geometry_nauwkeurigheid: GeometryNauwkeurigheid = GeometryNauwkeurigheid._50) -> None:
        """
        :param asset:
        :type asset: AssetDTO
        :param wkt_geometry: Well Known Text geometrie
        :type wkt_geometry: str
        :param geometry_log: default waarde "-1"
        :param geometry_bron: default waarde "Manueel"
        :param geometry_nauwkeurigheid: default waarde "<50 cm"
        :return: None
        """
        return self.add_geometrie_by_uuid(asset_uuid=asset.uuid, wkt_geometry=wkt_geometry, geometry_log=geometry_log,
                                          geometry_bron=geometry_bron, geometry_nauwkeurigheid=geometry_nauwkeurigheid)

    def update_geometrie_by_uuid(self, asset_uuid: str, wkt_geometry: str) -> None:
        """
        Update de bestaande geometrie eigenschap door 3 hulpfuncties in te roepen:
        - get_geometrie()
        - delete_geometrie()
        - add_geometrie()

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param wkt_geometry: Well Known Text representation of the geometrie
        :type wkt_geometry: str
        :return:
        """
        return self.update_geometrie_by_uuid(asset_uuid=asset_uuid, wkt_geometry=wkt_geometry)

    def update_geometrie(self, asset: AssetDTO, wkt_geometry: str) -> None:
        """
        Update de bestaande geometrie eigenschap door 3 hulpfuncties in te roepen:
        - get_geometrie()
        - delete_geometrie()
        - add_geometrie()

        :param asset:
        :param wkt_geometry: Well Known Text representation of the geometrie
        :return:
        """
        if not is_valid_wkt(wkt_string=wkt_geometry):
            raise ValueError(f'WKT Geometry is invalid: {wkt_geometry}.')

        # step 1: search existing geometry
        geometriekenmerk = self.get_geometrie_by_uuid(asset_uuid=asset)

        # step 2: remove existing geometry
        if geometriekenmerk.logs:
            if log_id := geometriekenmerk.logs[0].uuid:
                self.delete_geometrie_by_uuid(asset_uuid=asset, log_id=log_id)

        # step 3: add new geometry
        self.add_geometrie_by_uuid(asset_uuid=asset, wkt_geometry=wkt_geometry)