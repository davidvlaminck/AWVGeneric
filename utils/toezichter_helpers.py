from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import AssetDTO


def get_toezichter_naam(eminfra_client: EMInfraClient, asset: AssetDTO) -> str | None:
    """
    Returns the complete toezichter naam (voornaam + naam) from an asset.
    """
    toezichter_kenmerk = eminfra_client.get_kenmerk_toezichter_by_asset_uuid(
        asset_uuid=asset.uuid
    )
    if toezichter_kenmerk and toezichter_kenmerk.toezichter:
        toezichter_uuid = toezichter_kenmerk.toezichter.get("uuid")
        toezichter = eminfra_client.get_identiteit(toezichter_uuid=toezichter_uuid)
        return f'{toezichter.voornaam} {toezichter.naam}'
    else:
        return None