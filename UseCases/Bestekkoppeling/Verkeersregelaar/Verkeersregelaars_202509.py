import copy
import datetime
import logging

from API.eminfra.EMInfraDomain import BestekCategorieEnum, BestekKoppelingStatusEnum, BestekKoppeling
from utils.date_helpers import format_datetime
from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def import_data(filepath: Path):
    """Import Excel data in a Dataframe"""
    df = pd.read_excel(
        filepath,
        sheet_name='VRLegacy',
        header=0,
        usecols=["typeURI", "assetId.identificator", "isActief", "naam", "naampad", "toestand"]
    )
    df.rename(columns={"assetId.identificator": "uuid"}, inplace=True)
    df["uuid"] = df["uuid"].str.slice(0, 36)
    return df

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info("""
        Wijzigen Bestekkoppelingen voor Verkeersregelaars (Legacy)
        
        Het startbestand is een DAVIE-download van alle Verkeersregelaars (OTL) en de bijhorende Verkeersregelinstallatie (Legacy).
        
        Loop over de assets en zoek naar de assets wiens bestek naam start met "VWT/INN/2020/011_".
        
        Deactiveer dit bestek met als einddatum 1/9/2025.
        Activeer een nieuw bestek met als startdatum 1/1/2021.
        Kies voor Swarco of Yunex, infucntie van de aannemer van het laatste actieve bestek
                
         - VWT/INN/2020/011_004AWV/TLC_1 (Swarco)
         - VWT/INN/2020/011_004AWV/TLC_2 (Yunex)
    """)
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    bestekref_swarco = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_1')
    bestekref_yunex = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_2')

    df_assets = import_data(filepath=Path().home() / 'Downloads' / 'Verkeersregelaars' / 'VR_compleet_DA-2025-49894_export.xlsx')

    STARTDATE = format_datetime(datetime.datetime(year=2025, month=9, day=1))
    STARTDATE_2021 = format_datetime(datetime.datetime(year=2021, month=1, day=1))

    for idx, asset in df_assets.iterrows():
        logging.debug(f'Processing asset: {asset.uuid} (Legacy).')
        if pd.isna(asset.uuid):
            logging.debug('Skipping this record since OTL or LGC asset is unknown.')
            continue

        # Ophalen van de bestaande assets (AssetDTO)
        asset_lgc = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.get("uuid")), None)

        # Ophalen van de bestekken van de VRLegacy.
        bestekken_lgc = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid)
        bestekken_lgc_update = copy.deepcopy(bestekken_lgc)

        # Logica toepassen in functie van de aannemer (Swarco of Yunex).
        logging.debug('Controleer of 1 van de ACTIEVE bestekken, diens besteknummer start met de naam "VWT/INN/2020/011_".')
        for bestekkoppeling in bestekken_lgc_update:
            if bestekkoppeling.status == BestekKoppelingStatusEnum.ACTIEF and bestekkoppeling.bestekRef.eDeltaBesteknummer.startswith("VWT/INN/2020/011_") and bestekkoppeling.bestekRef.uuid not in (bestekref_yunex.uuid, bestekref_swarco.uuid):
                logging.debug('Controleer dat de startdatum van de bestekkoppeling in de toekomst ligt.')
                if bestekkoppeling.startDatum > STARTDATE:
                    logging.debug('Update startdatum van het bestek')
                    bestekkoppeling.startDatum = STARTDATE
                logging.info("Beëindig bestekkoppeling")
                bestekkoppeling.eindDatum = STARTDATE

                if bestekkoppeling.bestekRef.aannemerNaam == 'YUNEX':
                    bestekkoppeling_nieuw = BestekKoppeling(bestekRef=bestekref_yunex, startDatum=STARTDATE_2021,
                                                            categorie=BestekCategorieEnum.WERKBESTEK,
                                                            status=BestekKoppelingStatusEnum.ACTIEF)
                elif bestekkoppeling.bestekRef.aannemerNaam in ('SWARCO', 'SWARCO Belgium', 'TM SWARCO-FABRICOM'):
                    bestekkoppeling_nieuw = BestekKoppeling(bestekRef=bestekref_swarco, startDatum=STARTDATE_2021,
                                                            categorie=BestekCategorieEnum.WERKBESTEK,
                                                            status=BestekKoppelingStatusEnum.ACTIEF)
                else:
                    raise ValueError(f'Aannemer naam moet ofwel Yunex of Swarco zijn. (={bestekkoppeling.bestekRef.aannemerNaam})')

                logging.debug("Nieuwe bestekkoppeling van YUNEX/Swarco toevoegen op de eerste index-positie (als die nog niet bestaat)")
                if bestekken_lgc_update[0].bestekRef.uuid not in (bestekref_yunex.uuid, bestekref_swarco.uuid):
                    df_assets.loc[
                        idx, "nieuw_eDeltaDossiernummer"] = bestekkoppeling_nieuw.bestekRef.eDeltaDossiernummer
                    df_assets.loc[idx, "nieuw_startdatum"] = bestekkoppeling_nieuw.startDatum
                    df_assets.loc[idx, "nieuw_einddatum"] = bestekkoppeling_nieuw.eindDatum
                    df_assets.loc[idx, "nieuw_status"] = bestekkoppeling_nieuw.status.value

                    bestekken_lgc_update.insert(0, bestekkoppeling_nieuw)

                # Bestekkoppelingen updaten
                logging.info(f'Process asset: "{asset_lgc.uuid}". Updating bestekkoppelingen: {bestekken_lgc_update}.')
                eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid, bestekkoppelingen=bestekken_lgc_update)

                df_assets.loc[idx, "eDeltaDossiernummer"] = bestekkoppeling.bestekRef.eDeltaDossiernummer
                df_assets.loc[idx, "startdatum"] = bestekkoppeling.startDatum
                df_assets.loc[idx, "einddatum"] = bestekkoppeling.eindDatum
                df_assets.loc[idx, "status"] = bestekkoppeling.status.value

    # info tijdelijk wegschrijven om de huidige bestekkoppelingen beter te kunnen analyseren.
    df_assets.to_excel(Path().home() / 'Downloads' / 'Verkeersregelaars' / 'info_bestekkoppelingen_202509.xlsx'
                       , index=False
                       , freeze_panes=[1, 1]
                       )