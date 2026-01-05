import os
import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from pathlib import Path

print("""
    Dit script schrapt de bestekkoppelingen die gelinkt zijn aan een bestek zonder naam.
    De input is een lijst van assets, afkomstig van een datakwaliteitsrapport: Assets gelinkt aan bestekken zonder aannemer.
    
    Voor deze assets haalt men eerst de bestekkoppelingen op.
    Vervolgens schrapt men de bestekkoppelingen zonder naam. Men schrapt geen item uit de lijst (in-place), 
    maar kopieert de lijst en filtert op de bestekreferentie die overeen komt met diegene uit het data kwaliteits rapport.
    Tot slot laadt men de aangepaste bestekkoppelingen op.
    """)

if __name__ == '__main__':
    environment = Environment.PRD
    print(f'environment:\t\t{environment}')

    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    excel_path = Path.home() / 'Downloads' / 'bestekken' / '[RSA] Assets gelinkt aan bestekken zonder aannemer.xlsx'
    if not excel_path.exists():
        raise FileNotFoundError(f'Could nog found the file: {excel_path}')
    df_assets = pd.read_excel(excel_path, sheet_name='Resultaat', header=2, usecols=['uuid', 'naampad', 'naam', 'bestekuuid'])

    for index, asset in df_assets.iterrows():
        print(f'processing asset:\t\t{asset.uuid}')

        # get_bestekkoppelingen_by_asset_uuid
        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

        # delete one record of bestekkoppelingen.
        bestekkoppelingen_update = list(filter(lambda item: item.bestekRef.uuid != asset.bestekuuid, bestekkoppelingen))

        # verify that one bestekkoppeling is discarded and update the bestekkoppelingen
        nbr_bestekkoppelingen = len(bestekkoppelingen)
        nbr_bestekkoppelingen_update = len(bestekkoppelingen_update)

        if (nbr_bestekkoppelingen - nbr_bestekkoppelingen_update) == 1 and nbr_bestekkoppelingen_update >= 1:
            print(f'\t# bestekkoppelingen intial: {nbr_bestekkoppelingen}')
            print(f'\t# bestekkoppelingen new: {nbr_bestekkoppelingen_update}')

            eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid, bestekkoppelingen=bestekkoppelingen_update)