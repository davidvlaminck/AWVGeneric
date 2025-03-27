from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd

print(""""
        Edit the Serienummer from OTL- and LGC-assets: add leading zeros to the last three digits if missing.      
      """)

def correct_serienummer(serienummer: str):
    """
    Capitalize serienummer
    Add leading zero's to the last part of the serienummer, to ensure 3 digits

    :param serienummer:
    :return:
    """
    # capitalize serienummer
    serienummer = serienummer.upper()

    # Extract last part
    parts = serienummer.split('-')
    # Append leading zeros
    parts[-1] = parts[-1].zfill(3) # zfill up to 3 characters

    return '-'.join(parts)

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / ('[RSA] SegmentControllers hun serienummer volgt '
                                                                    'een bepaalde regex validatie.xlsx')

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "naam", "naampad", "serienummer", "otl_vs_lgc"])
    df_assets.drop_duplicates(inplace=True)

    ################################################################################
    ###  Update OTL- and LGC-asset
    ################################################################################
    for index, asset in df_assets.iterrows():
        asset_uuid = asset['uuid']
        print(f"Updating eigenschap 'serienummer' for asset: {asset_uuid}")

        ################################################################################
        ###  Get eigenschap
        ################################################################################
        # Eigenschap ophalen via API-call (eminfra)
        segment_controller = eminfra_client.get_asset_by_id(assettype_id=asset_uuid)

        eigenschappen = eminfra_client.get_eigenschappen(assetId=asset_uuid)
        if asset.otl_vs_lgc == 'OTL':
            eigenschap_serienummer = next(item for item in eigenschappen if item.eigenschap.naam == 'serienummer')
        elif asset.otl_vs_lgc == 'LGC':
            eigenschap_serienummer = next(
                item for item in eigenschappen if item.eigenschap.naam == 'serienummer segment controller')
        else:
            raise ValueError('Undefined asset type, OTL or LGC')

        # waarde updaten
        eigenschap_serienummer.typedValue['value'] = correct_serienummer(eigenschap_serienummer.typedValue['value'])

        #################################################################################
        ####  Get eigenschap_uuid
        #################################################################################
        if asset.otl_vs_lgc == 'OTL':
            eigenschap_generator = eminfra_client.search_eigenschappen(eigenschap_naam='serienummer')
            eigenschap_uuid = next(eigenschap.uuid for eigenschap in eigenschap_generator if eigenschap.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#SerienummerObject.serienummer')
        elif asset.otl_vs_lgc == 'LGC':
            eigenschap_generator = eminfra_client.search_eigenschappen(eigenschap_naam='serienummer segment controller')
            eigenschap_uuid = next(eigenschap.uuid for eigenschap in eigenschap_generator if eigenschap.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#SerienummerObject.serienummer')
        else:
            raise ValueError('Undefined asset type, OTL or LGC')

        #################################################################################
        ####  Get kenmerk_uuid
        #################################################################################
        kenmerken = eminfra_client.get_kenmerken(assetId=asset_uuid)
        kenmerk_uuid = next(
            kenmerk.type.get('uuid') for kenmerk in kenmerken if kenmerk.type.get('naam').startswith('Eigenschappen'))

        #################################################################################
        ####  Update eigenschap
        #################################################################################
        eminfra_client.update_eigenschap(assetId=asset_uuid, eigenschap=eigenschap_serienummer)