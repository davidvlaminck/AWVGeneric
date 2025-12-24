from API.EMInfraDomain import AssetDTO, ToezichterKenmerk, IdentiteitKenmerk, ToezichtgroepDTO


class ToezichterService:
    def __init__(self, requester):
        self.requester = requester
        self.TOEZICHTER_UUID = 'f0166ba2-757c-4cf3-bf71-2e4fdff43fa3'

    # todo:
    # test of bij het ToezichterKenmerk de eigenschappen toezichter en toezichtGroep kunnen vervangen worden door IdentiteitKenmerk en ToezichtgroepDTO
    def get_toezichter(self, asset_uuid: str) -> ToezichterKenmerk:
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.TOEZICHTER_UUID}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichterKenmerk.from_dict(response.json())

    def add_toezichter(self, asset_uuid: str, toezichtgroep_uuid: str, toezichter_uuid: str) -> None:
        """
        Both toezichter and toezichtsgroep must be updated simultaneously.
        Updating only one of both (toezichter/toezichtsgroep), purges the other.
        :param asset_uuid:
        :param toezichtgroep_uuid:
        :param toezichter_uuid:
        :return:
        """
        payload = {}
        if toezichter_uuid:
            payload["toezichter"] = {
                "uuid": toezichter_uuid
            }
        if toezichtgroep_uuid:
            payload["toezichtGroep"] = {
                "uuid": toezichtgroep_uuid
            }
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.TOEZICHTER_UUID}'
            , json=payload
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_identiteit(self, toezichter_uuid: str) -> IdentiteitKenmerk:
        response = self.requester.get(
            url=f'identiteit/api/identiteiten/{toezichter_uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return IdentiteitKenmerk.from_dict(response.json())

    def get_toezichtgroep(self, toezichtGroep_uuid: str) -> ToezichtgroepDTO:
        response = self.requester.get(
            url=f'identiteit/api/toezichtgroepen/{toezichtGroep_uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichtgroepDTO.from_dict(response.json())