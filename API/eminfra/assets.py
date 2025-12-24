from API.EMInfraDomain import AssetDTO


class AssetService:
    def __init__(self, requester):
        self.requester = requester

    def get_asset(self, asset_id: str) -> AssetDTO:
        url = f"core/api/assets/{asset_id}"
        json_dict = self.requester.get(url).json()
        return AssetDTO.from_dict(json_dict)