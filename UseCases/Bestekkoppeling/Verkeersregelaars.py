import datetime
from dateutil.relativedelta import relativedelta
import logging

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

print(""""
        Wijzigen Bestekkoppelingen voor Verkeersregelaars (Legacy)
        Voor de bijhorende Verkeersregelaar (OTL) de eigenschap datumOprichtingObject ophalen.
        Nieuwe startdatum van een bestekkoppeling 2 jaar inverder plaatsen (+ 2 jaar).
        Toevoegen van een nieuwe bestekkoppeling (Yunex/Swarco) op de eerste positie van alle bestekkoppelingen.
        Beëindigen van de bestaande bestekkoppeling op deze startdatum.
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def import_data():
    """Import Excel data in a Dataframe"""
    filepath = Path().home() / 'Downloads' / 'VerkeersRegelinstallatie' / 'VR_OTL-Legacy.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='VR_OTL-Legacy', header=0, usecols=["vr_otl_uuid", "vr_lgc_uuid"])
    return df_assets


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Verkeersregelaars:\tUpdate bestekkoppelingen')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = import_data()

    for df_index, asset in df_assets.iterrows():
        logging.debug(f'Processing assets: {asset.vr_otl_uuid} (OTL) en {asset.vr_lgc_uuid} (Legacy).')
        if pd.isna(asset.vr_otl_uuid) or pd.isna(asset.vr_lgc_uuid):
            logging.debug('Skipping this record since OTL or LGC asset is unknown.')
            continue

        # Ophalen van de bestaande assets (AssetDTO)
        asset_otl = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_otl_uuid), None)
        asset_lgc = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_lgc_uuid), None)

        # Ophalen van de eigenschap "datumOprichtingObject" van de VR (OTL)
        eigenschapwaarden = eminfra_client.get_eigenschapwaarden(assetId=asset_otl.uuid, eigenschap_naam='datumOprichtingObject')
        if len(eigenschapwaarden) != 1:
            logging.debug(f'Asset: {asset_otl.uuid} ontbreekt de eigenschap "datumOprichtingObject"')
        else:
            datumOprichtingObject = eigenschapwaarden[0].typedValue.get('value')

            # Parse datumOprichtingObject (text) to a datetime.
            datumOprichtingObject = datetime.datetime.strptime(datumOprichtingObject, "%Y-%m-%d").date()

            # Nieuwe bestekdatum = Eigenschap datumOprichtingObject + 2 jaar
            startdate = datumOprichtingObject + relativedelta(years=2)

        # Ophalen van de bestekken van de VRLegacy.
        bestekken_lgc = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid)

        # Info tijdelijk wegschrijven
        for idx, i in enumerate(bestekken_lgc):
            df_assets.at[df_index, f'bestek_{idx}_aannemernaam'] = i.bestekRef.aannemerNaam
            df_assets.at[df_index, f'bestek_{idx}_dossiernummer'] = i.bestekRef.eDeltaDossiernummer
            df_assets.at[df_index, f'bestek_{idx}_actief'] = i.bestekRef.actief
            df_assets.at[df_index, f'bestek_{idx}_data'] = f'{i.startDatum} {i.eindDatum}'

    # info tijdelijk wegschrijven om de huidige bestekkoppelingen beter te kunnen analyseren.
    df_assets.to_excel(Path().home() / 'Downloads' / 'VerkeersRegelinstallatie' / 'info_bestekkoppelingen.xlsx'
                       , index=False
                       , freeze_panes=[1,1]
                       )

        # Logica toepassen in functie van de aannemer (Swarco of Yunex).
        # todo logica implementeren

        # Bestekkoppelingen updaten (beëindigen bestaand bestek, nieuw bestek toevoegen).
        # bestekref_swarco = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_1')
        # bestekref_yunex = eminfra_client.get_bestekref_by_eDelta_besteknummer('VWT/INN/2020/011_004AWV/TLC_2')

        # Nieuw bestek vooraan plaatsen
        # bestekken_lgc.insert(0, bestekref_swarco)
        # bestekken_lgc.insert(0, bestekref_yunex)

        # Alle Bestekkoppelingen updaten
        # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid, bestekkoppelingen=bestekken_lgc)
