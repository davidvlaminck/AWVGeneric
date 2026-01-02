import json
import logging

from API.eminfra.eminfra_domain import LocatieKenmerk, AssetDTO
from API.eminfra.wkt_validator import is_valid_wkt


class LocatieService:
    def __init__(self, requester):
        self.requester = requester
        self.LOCATIE_UUID = '80052ed4-2f91-400c-8cba-57624653db11'

    def get_locatie(self, asset: AssetDTO) -> LocatieKenmerk:
        """
        Get LocatieKenmerk from an asset.
        :param asset:
        :type asset: AssetDTO
        :return: LocatieKenmerk
        :rtype:
        """
        response = self.requester.get(
            url=f'core/api/assets/{asset.uuid}/kenmerken/{self.LOCATIE_UUID}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return LocatieKenmerk.from_dict(response.json())

    def update_locatie(self, bronAsset: AssetDTO, doelAsset: AssetDTO = None, wkt_geometry: str = None) -> None:
        """
        Update locatie based on a WKT-string or via an existing relation
        Call this function with parameter doelAsset to set the locatie via an existing relationship.
        Provide parameter wkt_geom to set the location to a valid WKT-string.

        :param bronAsset:
        :type bronAsset: AssetDTO
        :param doelAsset:
        :type doelAsset: AssetDTO
        :param wkt_geometry: Well Known Text geometry
        :type wkt_geometry: str
        :return: None
        :rtype:
        """
        if not doelAsset and not wkt_geometry:
            raise ValueError(
                'At least one optional parameter "doel_asset_uuid" or "wkt_geom" should be provided.'
            )
        elif wkt_geometry:
            if not is_valid_wkt(wkt_string=wkt_geometry):
                raise ValueError(f'WKT Geometry is invalid: {wkt_geometry}.')
            return self._update_locatie_via_wkt(asset=bronAsset, wkt_geom=wkt_geometry)
        else:
            # to do: implement a check that the relation already exists between the bron- and doel-asset.
            return self._update_locatie_via_relatie(bronAsset=bronAsset, doelAsset=doelAsset)

    def _update_locatie_via_wkt(self, asset: AssetDTO, wkt_geom: str) -> None:
        """
        Update het kenmerk locatie via een WKT-string
        :param asset:
        :type asset: AssetDTO
        :param wkt_geom: Well Known Text
        :type wkt_geom: str
        :return: None
        :rtype:
        """
        json_body = {"geometrie": f"{wkt_geom}"}
        response = self.requester.put(
            url=f'core/api/assets/{asset.uuid}/kenmerken/{self.LOCATIE_UUID}/geometrie'
            , data=json.dumps(json_body)
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def _update_locatie_via_relatie(self, bronAsset: AssetDTO, doelAsset: AssetDTO) -> None:
        """
        Update het kenmerk locatie via een bestaande steun-relatie
        :param bronAsset:
        :type bronAsset: AssetDTO
        :param doelAsset:
        :type doelAsset: AssetDTO
        :return: None
        :rtype:
        """
        json_body = {
            "relatie": {
                "asset": {
                    "uuid": f'{doelAsset.uuid}',
                    "_type": "installatie"}}}
        response = self.requester.put(
            url=f'core/api/assets/{bronAsset.uuid}/kenmerken/{self.LOCATIE_UUID}'
            , data=json.dumps(json_body)
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))