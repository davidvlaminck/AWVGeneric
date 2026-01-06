import logging
from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

print(""""
        Ophalen van de eigenschap locatie van een asset
      """)


def load_settings():
    """Load API settings from JSON"""
    return (
            Path().home()
            / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    )


if __name__ == '__main__':
    # Create and configure logger
    logging.basicConfig(filename="debug.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')
    # Creating an object
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    settings_path = load_settings()
    excel_path = Path().home() / 'Downloads' / 'Teletransmissieverbinding'
    df_input = pd.read_excel(
        excel_path / 'input' / 'TT.ODF_met_potentiele_Kabelnettoegangen.xlsx',
        sheet_name='TT_ODF',
        header=0,
        usecols=[
            "typeURI",
            "assetId.identificator",
            "naam"
        ],
    )
    df_output = df_input.copy(deep=True)

    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    for idx, asset in df_input.iterrows():
        print(f'Search eigenschap "locatie" for asset Teletransmissieverbinding: {asset.get("assetId.identificator")}')

        ################################################################################
        ###  Get eigenschap locatie
        ################################################################################
        # Eigenschap locatie ophalen via API-call
        locatie = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.get("assetId.identificator"))
        if locatie.locatie is None:
            logger.debug(msg='Locatie not found', )
            gemeente, provincie = None, None
        elif adres := locatie.locatie.get("adres"):
            gemeente = adres.get('gemeente')
            provincie = adres.get('provincie')
        else:
            logger.debug(msg='Locatie not found')
            gemeente, provincie = None, None

        df_output.loc[idx, "gemeente"] = gemeente
        df_output.loc[idx, "provincie"] = provincie

    #################################################################################
    ####  Write df to Excel
    #################################################################################
    file_path = excel_path / 'output' / 'TT_met_locatie_info.xlsx'
    print(f'Write file to: {file_path}')
    df_output.to_excel(excel_writer=file_path, sheet_name='TT', index=False)
