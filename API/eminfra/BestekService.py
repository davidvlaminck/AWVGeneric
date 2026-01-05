import json
import logging
from datetime import datetime

from API.eminfra.EMInfraDomain import (BestekKoppeling, BestekRef, PagingModeEnum, SelectionDTO, OperatorEnum,
                                       ExpressionDTO, TermDTO, QueryDTO, BestekCategorieEnum,
                                       BestekKoppelingStatusEnum, AssetDTO)
from utils.date_helpers import validate_dates, format_datetime


class BestekService:
    def __init__(self, requester):
        self.requester = requester
        self.BESTEKKOPPELING_UUID = 'ee2e627e-bb79-47aa-956a-ea167d20acbd'

    def get_bestekkoppeling_by_uuid(self, asset_uuid: str) -> list[BestekKoppeling]:
        """
        Ophalen van de bestekkoppelingen, gelinkt aan een asset

        :param asset_uuid: Asset uuid
        :type asset: str
        :return: [Bestekkoppeling]
        """
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/{self.BESTEKKOPPELING_UUID}/bestekken')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [BestekKoppeling.from_dict(item) for item in response.json()['data']]

    def get_bestekkoppeling(self, asset: AssetDTO) -> list[BestekKoppeling]:
        """
        Ophalen van de bestekkoppelingen, gelinkt aan een asset

        :param asset: Asset
        :type asset: AssetDTO
        :return: [Bestekkoppeling]
        """
        return self.get_bestekkoppeling_by_uuid(asset_uuid=asset.uuid)

    def get_bestekref(self, eDelta_dossiernummer: str = None, eDelta_besteknummer: str = None) -> BestekRef | None:
        """
        Opzoeken van een BestekRef op basis van een dossiernummer of een besteknummer.

        :param eDelta_dossiernummer:
        :type eDelta_dossiernummer: str
        :param eDelta_besteknummer:
        :type eDelta_besteknummer: str
        :return: Bestekreferentie
        :rtype: BestekRef | None
        """
        if eDelta_dossiernummer:
            return self._get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
        elif eDelta_besteknummer:
            return self._get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer)
        else:
            raise ValueError('At least one optional input parameter eDelta_dossiernummer of eDelta_besteknummer must '
                             'be provided')

    def _get_bestekref_by_eDelta_dossiernummer(self, eDelta_dossiernummer: str) -> BestekRef | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaDossiernummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_dossiernummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        bestekrefs_list = [BestekRef.from_dict(item) for item in response.json()['data']]
        if len(bestekrefs_list) != 1:
            raise ValueError(
                f'Expected one single bestek for {eDelta_dossiernummer}. Got {len(bestekrefs_list)} instead.')
        return bestekrefs_list[0]

    def _get_bestekref_by_eDelta_besteknummer(self, eDelta_besteknummer: str) -> BestekRef | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaBesteknummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_besteknummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        bestekrefs_list = [BestekRef.from_dict(item) for item in response.json()['data']]
        if len(bestekrefs_list) != 1:
            raise ValueError(
                f'Expected one single bestek for {eDelta_besteknummer}. Got {len(bestekrefs_list)} instead.')
        return bestekrefs_list[0]


    def change_bestekkoppelingen_by_uuid(self, asset_uuid: str, bestekkoppelingen: [BestekKoppeling]) -> None:
        """

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param bestekkoppelingen: List of BestekKoppeling
        :type bestekkoppelingen: [BestekKoppeling]
        :return: None
        :rtype:
        """
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.BESTEKKOPPELING_UUID}/bestekken',
            data=json.dumps({'data': [item.asdict() for item in bestekkoppelingen]}))
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def change_bestekkoppelingen(self, asset: AssetDTO, bestekkoppelingen: [BestekKoppeling]) -> None:
        """

        :param asset: Asset
        :type asset: AssetDTO
        :param bestekkoppelingen: List of BestekKoppeling
        :type bestekkoppelingen: [BestekKoppeling]
        :return: None
        :rtype:
        """
        return self.change_bestekkoppelingen_by_uuid(asset_uuid=asset.uuid, bestekkoppelingen=bestekkoppelingen)

    def adjust_date_bestekkoppeling_by_uuid(self, asset_uuid: str, bestek_ref_uuid: str,
                                                  start_datetime: datetime = None,
                                                  end_datetime: datetime = None) -> dict | None:
        """
        Adjusts the startdate and/or the enddate of an existing bestekkoppeling.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param start_datetime: start-date of the bestekkoppeling, datetime
        :param end_datetime: end-date of the bestekkoppeling, datetime
        :return: response of the API call, or None when nothing is updated.
        """
        validate_dates(start_datetime=start_datetime, end_datetime=end_datetime)
        bestekkoppelingen = self.get_bestekkoppeling_by_uuid(asset_uuid=asset_uuid)
        if matching_koppeling := next(
                (
                        k
                        for k in bestekkoppelingen
                        if k.bestekRef.uuid == bestek_ref_uuid
                ),
                None,
        ):
            if start_datetime:
                matching_koppeling.startDatum = format_datetime(start_datetime)
            if end_datetime:
                matching_koppeling.eindDatum = format_datetime(end_datetime)
        return self.change_bestekkoppelingen_by_uuid(asset_uuid, bestekkoppelingen)

    def adjust_date_bestekkoppeling(self, asset: AssetDTO, bestek_ref_uuid: str, start_datetime: datetime = None,
                                    end_datetime: datetime = None) -> dict | None:
        """
        Adjusts the startdate and/or the enddate of an existing bestekkoppeling.

        :param asset: Asset
        :type asset: AssetDTO
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param start_datetime: start-date of the bestekkoppeling, datetime
        :param end_datetime: end-date of the bestekkoppeling, datetime
        :return: response of the API call, or None when nothing is updated.
        """
        return self.adjust_date_bestekkoppeling_by_uuid(asset_uuid=asset.uuid, bestek_ref_uuid=bestek_ref_uuid,
                                                        start_datetime=start_datetime, end_datetime=end_datetime)

    def end_bestekkoppeling_by_uuid(self, asset_uuid: str, bestek_ref_uuid: str,
                            end_datetime: datetime = datetime.now()) -> dict | None:
        """
        End a bestekkoppeling by setting an enddate. Defaults to the actual date of execution.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param end_datetime: end-date of the bestek
        :return: response of the API call, or None when nothing is updated.
        """
        end_datetime = format_datetime(end_datetime)
        bestekkoppelingen = self.get_bestekkoppeling_by_uuid(asset_uuid=asset_uuid)
        if matching_koppeling := next(
                (
                        k
                        for k in bestekkoppelingen
                        if k.bestekRef.uuid == bestek_ref_uuid
                ),
                None,
        ):
            matching_koppeling.eindDatum = end_datetime
        return self.change_bestekkoppelingen_by_uuid(asset_uuid, bestekkoppelingen)

    def end_bestekkoppeling(self, asset: AssetDTO, bestek_ref_uuid: str,
                            end_datetime: datetime = datetime.now()) -> dict | None:
        """
        End a bestekkoppeling by setting an enddate. Defaults to the actual date of execution.

        :param asset: Asset
        :type asset: AssetDTO
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param end_datetime: end-date of the bestek
        :return: response of the API call, or None when nothing is updated.
        """
        return self.end_bestekkoppeling_by_uuid(asset_uuid=asset.uuid, bestek_ref_uuid=bestek_ref_uuid,
                                                end_datetime=end_datetime)

    def add_bestekkoppeling_by_uuid(self, asset_uuid: str, eDelta_besteknummer: str = None, eDelta_dossiernummer: str = None,
                            start_datetime: datetime = datetime.now(), end_datetime: datetime = None,
                            categorie: str = BestekCategorieEnum.WERKBESTEK, insert_index: int = 0) -> dict | None:
        """
        Add a new bestekkoppeling. Start date default the execution date. End date default open-ended.
        The optional parameters "eDelta_besteknummer" and "eDelta_dossiernummer" are mutually exclusive,
         meaning that one of both optional parameters must be provided.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param eDelta_besteknummer: besteknummer
        :param eDelta_dossiernummer: dossiernummer
        :param start_datetime: start-date of the bestek. Default None > actual date.
        :param end_datetime: end-date of the bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        if (eDelta_besteknummer is None) == (eDelta_dossiernummer is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer' or 'eDelta_dossiernummer' must be provided.")
        elif eDelta_besteknummer:
            new_bestekRef = self.get_bestekref(eDelta_besteknummer=eDelta_besteknummer)
        else:
            new_bestekRef = self.get_bestekref(eDelta_dossiernummer=eDelta_dossiernummer)

        # Format the start_date, or set actual date if None
        start_datetime = format_datetime(start_datetime)

        # Format the end_date if present
        if end_datetime:
            end_datetime = format_datetime(end_datetime)

        bestekkoppelingen = self.get_bestekkoppeling_by_uuid(asset_uuid)

        # Check if the new bestekkoppeling doesn't exist and append at the first index position, else do nothing
        if not (matching_koppeling := next(
                (k for k in bestekkoppelingen if k.bestekRef.uuid == new_bestekRef.uuid),
                None, )):
            new_bestekkoppeling = BestekKoppeling(
                bestekRef=new_bestekRef,
                status=BestekKoppelingStatusEnum.ACTIEF,
                startDatum=start_datetime,
                eindDatum=end_datetime,
                categorie=categorie
            )
            # Insert the new bestekkoppeling at the first index position.
            bestekkoppelingen.insert(insert_index, new_bestekkoppeling)

            return self.change_bestekkoppelingen_by_uuid(asset_uuid, bestekkoppelingen)

    def add_bestekkoppeling(self, asset: AssetDTO, eDelta_besteknummer: str = None, eDelta_dossiernummer: str = None,
                            start_datetime: datetime = datetime.now(), end_datetime: datetime = None,
                            categorie: str = BestekCategorieEnum.WERKBESTEK, insert_index: int = 0) -> dict | None:
        """
        Add a new bestekkoppeling. Start date default the execution date. End date default open-ended.
        The optional parameters "eDelta_besteknummer" and "eDelta_dossiernummer" are mutually exclusive,
         meaning that one of both optional parameters must be provided.

        :param asset: Asset
        :type asset: AssetDTO
        :param eDelta_besteknummer: besteknummer
        :param eDelta_dossiernummer: dossiernummer
        :param start_datetime: start-date of the bestek. Default None > actual date.
        :param end_datetime: end-date of the bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        return self.add_bestekkoppeling_by_uuid(asset_uuid=asset.uuid, eDelta_besteknummer=eDelta_besteknummer,
                                                eDelta_dossiernummer=eDelta_dossiernummer,
                                                start_datetime=start_datetime, end_datetime=end_datetime,
                                                categorie=categorie, insert_index=insert_index)

    def replace_bestekkoppeling_by_uuid(self, asset_uuid: str, eDelta_besteknummer_old: str = None,
                                eDelta_dossiernummer_old: str = None, eDelta_besteknummer_new: str = None,
                                eDelta_dossiernummer_new: str = None, start_datetime: datetime = datetime.now(),
                                end_datetime: datetime = None,
                                categorie: BestekCategorieEnum = BestekCategorieEnum.WERKBESTEK) -> dict | None:
        """
        Replaces an existing bestekkoppeling: ends the existing bestekkoppeling and add a new one.

        Call the functions end_bestekkoppeling and add_bestekkoppeling respectively
        The optional parameters "eDelta_besteknummer[old|new]" and "eDelta_dossiernummer[old|new]" are mutually exclusive, meaning that one of both optional parameters must be provided.
        The optional parameter start_datetime is the enddate of the existing bestek and the start date of the new bestek. Default value is the actual date.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param eDelta_besteknummer_old: besteknummer existing
        :param eDelta_dossiernummer_old: dossiernummer existing
        :param eDelta_besteknummer_new: besteknummer new
        :param eDelta_dossiernummer_new: dossiernummer new
        :param start_datetime: start-date of the new bestek, and end-date of the existing bestek. Default None > actual date.
        :param end_datetime: end-date of the new bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        # End bestekkoppeling
        if (eDelta_besteknummer_old is None) == (
                eDelta_dossiernummer_old is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer_old' or 'eDelta_dossiernummer_old' must be provided.")
        elif eDelta_besteknummer_old:
            bestekref = self.get_bestekref(eDelta_besteknummer=eDelta_besteknummer_old)
        else:
            bestekref = self.get_bestekref(eDelta_dossiernummer=eDelta_dossiernummer_old)

        self.end_bestekkoppeling_by_uuid(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref.uuid, end_datetime=start_datetime)

        # Add bestekkoppeling
        if (eDelta_besteknummer_new is None) == (
                eDelta_dossiernummer_new is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer_new' or 'eDelta_dossiernummer_new' must be provided.")
        else:
            self.add_bestekkoppeling_by_uuid(asset_uuid=asset_uuid, eDelta_besteknummer=eDelta_besteknummer_new,
                                             eDelta_dossiernummer=eDelta_dossiernummer_new,
                                             start_datetime=start_datetime, end_datetime=end_datetime,
                                             categorie=categorie)

    def replace_bestekkoppeling(self, asset: AssetDTO, eDelta_besteknummer_old: str = None,
                                eDelta_dossiernummer_old: str = None, eDelta_besteknummer_new: str = None,
                                eDelta_dossiernummer_new: str = None, start_datetime: datetime = datetime.now(),
                                end_datetime: datetime = None,
                                categorie: BestekCategorieEnum = BestekCategorieEnum.WERKBESTEK) -> dict | None:
        """
        Replaces an existing bestekkoppeling: ends the existing bestekkoppeling and add a new one.

        Call the functions end_bestekkoppeling and add_bestekkoppeling respectively
        The optional parameters "eDelta_besteknummer[old|new]" and "eDelta_dossiernummer[old|new]" are mutually exclusive, meaning that one of both optional parameters must be provided.
        The optional parameter start_datetime is the enddate of the existing bestek and the start date of the new bestek. Default value is the actual date.

        :param asset: Asset
        :type asset: AssetDTO
        :param eDelta_besteknummer_old: besteknummer existing
        :param eDelta_dossiernummer_old: dossiernummer existing
        :param eDelta_besteknummer_new: besteknummer new
        :param eDelta_dossiernummer_new: dossiernummer new
        :param start_datetime: start-date of the new bestek, and end-date of the existing bestek. Default None > actual date.
        :param end_datetime: end-date of the new bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        return self.replace_bestekkoppeling_by_uuid(asset_uuid=asset.uuid,
                                                    eDelta_besteknummer_old=eDelta_besteknummer_old,
                                                    eDelta_dossiernummer_old=eDelta_dossiernummer_old,
                                                    eDelta_besteknummer_new=eDelta_besteknummer_new,
                                                    eDelta_dossiernummer_new=eDelta_dossiernummer_new,
                                                    start_datetime=start_datetime, end_datetime=end_datetime,
                                                    categorie=categorie)