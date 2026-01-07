import logging
from datetime import datetime
from collections.abc import Generator

from API.eminfra.EMInfraDomain import (PostitDTO, QueryDTO, PagingModeEnum, SelectionDTO, OperatorEnum,
                                       LogicalOpEnum)
from utils.date_helpers import validate_dates, format_datetime
from utils.query_dto_helpers import add_expression


class PostitService:
    def __init__(self, requester):
        self.requester = requester

    def search_postits_generator(self, asset_uuid: str, start_datum: datetime = None,
                       eind_datum: datetime = None) -> Generator[PostitDTO] | None:
        """
        Search postits of an asset.
        If the optional parameters startDatum or eindDatum are missing, return all postits.

        :param asset_uuid: asset uuid
        :type asset_uuid: str
        :param start_datum: start date of the postit, default None
        :type start_datum: datetime
        :param eind_datum: eind date of the postit, default None
        :type eind_datum: datetime
        :return: Generator[PostitDTO] or None
        """
        # intiate empty expression
        query_dto = QueryDTO(
            size=5,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[]
            )
        )

        if start_datum:
            add_expression(query_dto, 'startDatum', OperatorEnum.GTE, start_datum)

        if eind_datum:
            add_expression(query_dto, 'eindDatum', OperatorEnum.LTE, eind_datum)

        # If both dates are present, add logical AND
        if start_datum and eind_datum:
            query_dto.selection.expressions[-1].logicalOp = LogicalOpEnum.AND

        if query_dto.size is None:
            query_dto.size = 100
        url = f"core/api/assets/{asset_uuid}/postits/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [PostitDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def get_postit(self, asset_uuid: str, postit_uuid: str) -> PostitDTO | None:
        """
        Search one postit of an asset.

        :param asset_uuid: asset uuid
        :type asset_uuid: str
        :param postit_uuid: postit_uuid
        :type postit_uuid: str
        :return: PostitDTO or None
        """
        url = f"core/api/assets/{asset_uuid}/postits/{postit_uuid}"

        response = self.requester.get(url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return PostitDTO.from_dict(response.json())

    def add_postit(self, asset_uuid: str, commentaar: str, start_datum: datetime, eind_datum: datetime) -> dict:
        """
        Add postit to an asset.

        :param asset_uuid: asset uuid
        :param commentaar: comment
        :param start_datum: start date of the postit
        :param eind_datum: end date of the postit
        :return: dict
        """
        validate_dates(start_datetime=start_datum, end_datetime=eind_datum)

        startDatum_str = format_datetime(start_datum)
        eindDatum_str = format_datetime(eind_datum)

        json_body = {
            "commentaar": commentaar,
            "startDatum": startDatum_str,
            "eindDatum": eindDatum_str
        }

        url = f"core/api/assets/{asset_uuid}/postits"
        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def edit_postit(self, asset_uuid: str, postit_uuid: str, commentaar: str = None, start_datum: datetime = None,
                    eind_datum: datetime = None) -> dict:
        """
        Edit postit of an asset.
        Although mandatory in the API Call, the parameters commentaar, startDatum and eindDatum are optional.
        When missing, the actual values are used

        Also used to perform a safe-delete, by altering only the parameter eindDatum.

        :param asset_uuid: asset
        :param postit_uuid: postit_uuid
        :param commentaar: comment
        :param start_datum: start date of the postit
        :param eind_datum: end date of the postit
        :return: dict
        """
        if start_datum and eind_datum:
            validate_dates(start_datetime=start_datum, end_datetime=eind_datum)

        actual_postit = self.get_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid)
        actual_commentaar = actual_postit.commentaar
        actual_startDatum = actual_postit.startDatum
        actual_eindDatum = actual_postit.eindDatum

        json_body = {"commentaar": commentaar if commentaar else actual_commentaar}

        if start_datum:
            startDatum_str = format_datetime(start_datum)
            json_body["startDatum"] = startDatum_str
        else:
            json_body["startDatum"] = actual_startDatum

        if eind_datum:
            eindDatum_str = format_datetime(eind_datum)
            json_body["eindDatum"] = eindDatum_str
        else:
            json_body["eindDatum"] = actual_eindDatum

        url = f"core/api/assets/{asset_uuid}/postits/{postit_uuid}"
        response = self.requester.put(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def remove_postit(self, asset_uuid: str, postit_uuid: str) -> dict:
        """
        Remove postit of an asset.

        :param asset_uuid: asset uuid
        :param postit_uuid: postit_uuid
        :return: dict
        """
        json_body = {
            "uuids": [f"{postit_uuid}"]
        }

        url = f"core/api/assets/{asset_uuid}/postits/ops/remove"
        response = self.requester.put(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response
