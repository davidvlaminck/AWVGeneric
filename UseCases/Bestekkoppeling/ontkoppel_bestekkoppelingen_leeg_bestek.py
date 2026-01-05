import os
import pandas as pd

from pathlib import Path

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    filepath = Path(r"C:\Users\DriesVerdoodtNordend\Downloads\[RSA] Assets gelinkt aan bestekken zonder aannemer.xlsx")
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2,
                              usecols=["uuid", "assettype", "naampad"])

    environment = Environment.PRD
    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    print("\tStart updating bestekkoppelingen")
    for index, asset in df_assets.iterrows():
        # Ophalen van alle bestekkoppelingen
        asset_uuid = asset.uuid
        print(f'Asset uuid: {asset_uuid}')

        myAsset = eminfra_client.assets.get_asset(asset_uuid=asset_uuid)
        bestekkoppelingen = eminfra_client.bestekken.get_bestekkoppeling(asset=myAsset)

        # Hoeveel bestekkoppelingen zijn er aanwezig? Meer dan 1.
        if len(bestekkoppelingen) <= 1:
            raise ValueError("0 or 1 bestekkoppeling found")

        # Testen dat exact 1 bestekkoppeling geen waardes heeft voor bepaalde keys
        matching_elements = [
            item for item in bestekkoppelingen
            if (item.bestekRef.aannemerNaam is None
                and
                item.bestekRef.aannemerReferentie is None
                and
                item.bestekRef.eDeltaBesteknummer is None
                and
                item.bestekRef.eDeltaDossiernummer is None
                )
        ]

        if len(matching_elements) != 1:
            raise ValueError("Meerdere bestekkoppelingen zonder waardes")

        # Er zijn minstens 2 bestekkoppelingen, waarvan maximum 1 zonder waardes. Als deze voorwaardes zijn vervuld, kan de bestekkoppeling gedeactiveerd worden.
        # Deactiveer de bestekkoppeling zonder waarde
        bestek_ref_uuid = matching_elements[0].bestekRef.uuid
        eminfra_client.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestek_ref_uuid) # set no end_date. Ends the bestek today.