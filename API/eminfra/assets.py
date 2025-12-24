from API.EMInfraDomain import AssetDTO, AssetDTOToestand


class AssetService:
    def __init__(self, requester):
        self.requester = requester

    def get_asset(self, asset_id: str) -> AssetDTO:
        url = f"core/api/assets/{asset_id}"
        json_dict = self.requester.get(url).json()
        return AssetDTO.from_dict(json_dict)

    def _update_asset(self, asset: AssetDTO, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None, commentaar: str = None) -> dict:
        # default bestaande waardes van de Asset.
        json_body = {
            "naam": asset.naam,
            "actief": asset.actief,
            "toestand": asset.toestand.value,
            "commentaar": asset.commentaar
        }
        # update asset eigenschappen naam, actief, toestand en commentaar
        if naam:
            json_body["naam"] = naam
        if actief:
            json_body["actief"] = actief
        if toestand:
            json_body["toestand"] = toestand.value
        if commentaar:
            json_body["commentaar"] = commentaar
        response = self.requester.put(
            url=f'core/api/assets/{asset.uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def update_asset(self, asset: AssetDTO, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None,
                     commentaar: str = None) -> dict:
        return self._update_asset(asset=asset, naam=naam, actief=actief, toestand=toestand, commentaar=commentaar)

    def update_toestand(self, asset: AssetDTO, toestand: AssetDTOToestand = AssetDTOToestand.IN_ONTWERP) -> dict:
        """
        Update toestand of an asset.

        :param asset:
        :param toestand:
        :return:
        """
        return self._update_asset(asset=asset, toestand=toestand)

    def update_commentaar(self, asset: AssetDTO, commentaar: str) -> dict:
        """
        Update commentaar of an asset.

        :param asset:
        :param commentaar:
        :return:
        """
        return self._update_asset(asset=asset, commentaar=commentaar)


    def activeer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=AssetDTO, actief=True)

    def deactiveer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=AssetDTO, actief=False)
