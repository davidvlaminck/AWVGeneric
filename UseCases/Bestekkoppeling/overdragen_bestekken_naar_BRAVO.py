import pandas as pd
import json
import os
from datetime import datetime

from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment


def collect_child_assets(asset_uuid, eminfra_client, all_assets=None, all_uuids=None):
    """
    Collects all assets from a Beheerobject in a recursive function call.

    :param asset_uuid: parent asset uuid, this is typically the uuid of the "Beheerobject"
    :param eminfra_client:
    :param all_assets: list to collect all the asset objects
    :param all_uuids:  list to collect all the uuids
    :return:
    """
    if all_assets is None:
        all_assets = []
    if all_uuids is None:
        all_uuids = []

    child_assets = eminfra_client.search_child_assets(asset_uuid=asset_uuid)

    for child_asset in child_assets:
        all_assets.append(child_asset)
        all_uuids.append(child_asset.uuid)
        collect_child_assets(child_asset.uuid, eminfra_client, all_assets, all_uuids)  # Recursive call

    return all_assets

def log_action(beheerobject: str, uuid: str, message: str = ""):
    """Logs an action into the DataFrame."""
    global log_df
    log_df = pd.concat([log_df, pd.DataFrame([{
        "Beheerobject": beheerobject,
        "uuid": uuid,
        "message": message
    }])], ignore_index=True)

if __name__ == '__main__':
    # initialize variables
    # Initialize an empty DataFrame for logging
    log_df = pd.DataFrame(columns=["Beheerobject", "uuid", "message"])

    start_datetime = datetime(2025, 3, 1)
    eDelta_dossiernummer_new = 'INTERN-5904'
    with open('beheerobjecten_BRAVO4.json', 'r') as file:
        beheerobjecten = json.load(file).get('beheerobjecten')

    environment = Environment.PRD
    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)
    print("Update de bestekkoppelingen voor assets die gelinkt zijn aan het project BRAVO4.")
    print("Beëindig het huidige bestek en installeer een nieuw bestek: INTERN-5904")
    print(f"De einddatum van het vorige bestek is tevens de startdatum van het nieuwe bestek: {start_datetime}")

    print(f"Initializing an EMInfraClient on the {environment} environment")

    # ophalen van de het Beheerobject op basis van de naam.

    # beheerojbect ophalen op basis van het de uuid van het Beheerobject

    for beheerobject in beheerobjecten:
        beheerobject_uuid = beheerobject.get("uuid")
        beheerobject_asset = eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=beheerobject_uuid)
        log_action(beheerobject_asset.naam, beheerobject_uuid, f"Wijzigen bestekkoppelingen voor alle assets in de boomstructuur van Beheerobject: {beheerobject_asset.naam}")

        # verzamel alle child assets in de boomstructuur
        all_assets = collect_child_assets(beheerobject_uuid, eminfra_client)


        for asset in all_assets:
            bestekkoppelingen = list(eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid))
            # zoek en beëindig alle actieve bestekkoppelingen
            bestekkoppelingen_actief = [bestekkoppeling for bestekkoppeling in bestekkoppelingen if
                                           bestekkoppeling.status == BestekKoppelingStatusEnum.ACTIEF]
            log_action(beheerobject_asset.naam, asset.uuid, f"Asset heeft {len(bestekkoppelingen_actief)} actieve bestekkoppelingen.")
            for bestekkoppeling_actief in bestekkoppelingen_actief:
                if bestekkoppeling_actief.bestekRef.eDeltaDossiernummer != eDelta_dossiernummer_new: # uitzondering voor BRAVO4, die mag niet gedeactiveerd worden.
                    print(f'Beëindig de actuele actieve bestekkoppeling voor eDeltadossiernummer {bestekkoppeling_actief.bestekRef.eDeltaDossiernummer}, assigned to asset: {asset.uuid}')
                    log_action(beheerobject_asset.naam, asset.uuid, f'beëindig de actuele actieve bestekkoppeling voor eDeltadossiernummer {bestekkoppeling_actief.bestekRef.eDeltaDossiernummer}')
                    eminfra_client.end_bestekkoppeling(asset_uuid=asset.uuid, bestek_ref_uuid=bestekkoppeling_actief.bestekRef.uuid, end_datetime=start_datetime)

            # add_bestekkoppeling
            print(f'Toewijzen van de bestekkoppeling eDeltadossiernummer {eDelta_dossiernummer_new} aan asset: {asset.uuid}')
            log_action(beheerobject_asset.naam, asset.uuid, f'Toewijzen van de bestekkoppeling eDeltadossiernummer {eDelta_dossiernummer_new}')
            eminfra_client.add_bestekkoppeling(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer_new, start_datetime=start_datetime)

    # Save log DataFrame at the end
    log_df.to_excel("log.xlsx", index=False)
    print("Log saved to log.xlsx")