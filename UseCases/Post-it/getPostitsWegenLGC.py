import pandas as pd
from pathlib import Path

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    # intiate eminfra_client
    environment = Environment.PRD
    print(f'environment:\t\t{environment}')

    settings_path = Path.home() / "OneDrive - Nordend" / "projects/AWV/resources/settings_SyncOTLDataToLegacy.json"
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # read csv in pandas dataframe
    csv_filepath = Path.home() / 'Downloads' / 'wegen_20250212.csv'
    csv_filepath_output = Path.home() / 'Downloads' / 'wegen_incl_postits_20250212.csv'
    df_assets = pd.read_csv(csv_filepath, sep=';')
    asset_uuids = df_assets.loc[:, "uuid"]

    # search for postits
    for asset_uuid in asset_uuids:
        # search_postits
        postits_generator = eminfra_client.search_postits(asset_uuid=asset_uuid)
        postits_generator_list = list(postits_generator)

        if postits_generator_list:
            print(f"postit-found! asset_uuid: {asset_uuid}")
            # append post-it information on the initial pandas dataframe
            df_assets.loc[df_assets['uuid'] == asset_uuid, 'postit'] = '; '.join(['startDatum: ' + postit.startDatum + ' eindDatum: '  + postit.eindDatum + ' Commentaar: ' + postit.commentaar for postit in postits_generator_list])

    df_assets.to_csv(csv_filepath_output, sep=';')