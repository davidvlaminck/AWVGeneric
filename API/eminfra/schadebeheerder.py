from API.EMInfraDomain import (SchadebeheerderKenmerk, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO,
                               TermDTO, OperatorEnum, AssetDTO)
from API.eminfra.kenmerken import KenmerkService


class SchadebeheerderService:
    def __init__(self, requester):
        self.requester = requester
        self.kenmerken = KenmerkService(self.requester)
        self.SCHADEBEHEERDER_UUID = 'd911dc02-f214-4f64-9c46-720dbdeb0d02'

    def get_schadebeheerder(self, asset: AssetDTO = None, name: str = None) -> [SchadebeheerderKenmerk] | None:
        if asset:
            return self._get_schadebeheerder_by_uuid(asset_uuid=asset.uuid)
        elif name:
            return self._get_schadebeheerder_by_name(name=name)
        else:
            raise ValueError('At least one optional parameter asset_uuid or name is mandatory')

    def _get_schadebeheerder_by_uuid(self, asset: AssetDTO) -> SchadebeheerderKenmerk | None:
        data = self.kenmerken.get(asset.uuid, self.SCHADEBEHEERDER_UUID)
        if sb := data.get("schadeBeheerder"):
            return [SchadebeheerderKenmerk.from_dict(sb)]
        return None

    def _get_schadebeheerder_by_name(self, name: str) -> SchadebeheerderKenmerk | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='naam',
                                                        operator=OperatorEnum.EQ,
                                                        value=f'{name}')])
                                 ]))
        response = self.requester.post(
            url='core/api/beheerders/search', data=query_dto.json())
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [SchadebeheerderKenmerk.from_dict(item) for item in response.json()['data']]

    def add_schadebeheerder(self, asset: AssetDTO, schadebeheerder: SchadebeheerderKenmerk) -> None:
        """
        Toevoegen van een schadebeheerder aan een asset.
        :param asset_uuid:
        :param schadebeheerder: SchadebeheerderKenmerk
        :return:
        """
        payload = {"schadeBeheerder": {"uuid": schadebeheerder.uuid}}
        self.kenmerken.put(asset.uuid, self.SCHADEBEHEERDER_UUID, payload)