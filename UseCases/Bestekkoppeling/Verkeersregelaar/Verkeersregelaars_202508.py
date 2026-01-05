import copy
import datetime
import logging
from utils.date_helpers import format_datetime
from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def import_data(filepath: Path):
    """Import Excel data in a Dataframe"""
    return pd.read_excel(
        filepath,
        sheet_name='Verkeersregelaar',
        header=0,
        usecols=["vr_otl_uuid", "vr_lgc_uuid"],
    )


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info("""
        Wijzigen Bestekkoppelingen voor Verkeersregelaars (Legacy)
        
        Het startbestand is een DAVIE-download van alle Verkeersregelaars (OTL) en de bijhorende Verkeersregelinstallatie (Legacy).
        
        Voor de bestaande bestekkoppelingen update ik de startdatum naar vandaag.
         - VWT/INN/2020/011_004AWV/TLC_1 (Swarco)
         - VWT/INN/2020/011_004AWV/TLC_2 (Yunex)
    """)
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    bestekref_swarco = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_1')
    bestekref_yunex = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_2')

    df_assets = import_data(filepath=Path().home() / 'Downloads' / 'VerkeersRegelinstallatie' / 'VR_compleet_DA-2025-46331_export.xlsx')

    STARTDATE = format_datetime(datetime.datetime(year=2025, month=8, day=12))

    for idx, asset in df_assets.iterrows():
        logging.debug(f'Processing assets: {asset.vr_otl_uuid} (OTL) en {asset.vr_lgc_uuid} (Legacy).')
        if pd.isna(asset.vr_otl_uuid) or pd.isna(asset.vr_lgc_uuid):
            logging.debug('Skipping this record since OTL or LGC asset is unknown.')
            continue

        # Ophalen van de bestaande assets (AssetDTO)
        asset_otl = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_otl_uuid), None)
        asset_lgc = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_lgc_uuid), None)

        # Ophalen van de bestekken van de VRLegacy.
        bestekken_lgc = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid)
        bestekken_lgc_update = copy.deepcopy(bestekken_lgc)

        # Logica toepassen in functie van de aannemer (Swarco of Yunex).
        logging.debug('Controleer of 1 van de ACTIEVE bestekken swarco of yunex is.')
        for bestekkoppeling in bestekken_lgc_update:
            if bestekkoppeling.bestekRef.uuid in (bestekref_yunex.uuid, bestekref_swarco.uuid):
                logging.info('Bestekkoppeling YUNEX/SWARCO aanwezig')

                logging.debug('Controleer dat de startdatum van de bestekkoppeling in de toekomst ligt.')
                if bestekkoppeling.startDatum > STARTDATE:
                    logging.debug('Update startdatum van het bestek')
                    bestekkoppeling.startDatum = STARTDATE

                    # Bestekkoppelingen updaten
                    logging.info(f'Process asset: "{asset_lgc.uuid}". Updating bestekkoppelingen.')

                    eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid, bestekkoppelingen=bestekken_lgc_update)

                df_assets.loc[idx, "eDeltaDossiernummer"] = bestekkoppeling.bestekRef.eDeltaDossiernummer
                df_assets.loc[idx, "startdatum"] = bestekkoppeling.startDatum

                logging.debug("Maximum 1 bestekkoppeling Yunex/Swarco teruggevonden. Stap uit de loop.")
                break

    # info tijdelijk wegschrijven om de huidige bestekkoppelingen beter te kunnen analyseren.
    df_assets.to_excel(Path().home() / 'Downloads' / 'VerkeersRegelinstallatie' / 'info_bestekkoppelingen_202508.xlsx'
                       , index=False
                       , freeze_panes=[1, 1]
                       )