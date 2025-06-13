import copy
import datetime
from dateutil.relativedelta import relativedelta
import logging
from utils.date_helpers import format_datetime
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import BestekKoppeling, BestekCategorieEnum, AssetDTO, BestekKoppelingStatusEnum
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


def get_startdate(asset: AssetDTO) -> datetime.datetime:
    start_date = None
    # Ophalen van de eigenschap "datumOprichtingObject" van de VR (OTL)
    eigenschapwaarden = eminfra_client.get_eigenschapwaarden(assetId=asset.uuid,
                                                             eigenschap_naam='datumOprichtingObject')
    if len(eigenschapwaarden) != 1:
        logging.debug(f'Asset: {asset.uuid} ontbreekt de eigenschap "datumOprichtingObject"')
    else:
        datumOprichtingObject = eigenschapwaarden[0].typedValue.get('value')

        # Parse datumOprichtingObject (text) to a datetime.
        datumOprichtingObject = datetime.datetime.strptime(datumOprichtingObject, "%Y-%m-%d")

        # Nieuwe bestekdatum = Eigenschap datumOprichtingObject + 2 jaar
        start_date = datumOprichtingObject + relativedelta(years=2)
    return start_date


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info('Verkeersregelaars:\tUpdate bestekkoppelingen')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    bestekref_swarco = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_1')
    bestekref_yunex = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/INN/2020/011_004AWV/TLC_2')

    df_assets = import_data()

    for df_index, asset in df_assets.iterrows():
        logging.debug(f'Processing assets: {asset.vr_otl_uuid} (OTL) en {asset.vr_lgc_uuid} (Legacy).')
        if pd.isna(asset.vr_otl_uuid) or pd.isna(asset.vr_lgc_uuid):
            logging.debug('Skipping this record since OTL or LGC asset is unknown.')
            continue

        # Ophalen van de bestaande assets (AssetDTO)
        asset_otl = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_otl_uuid), None)
        asset_lgc = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.vr_lgc_uuid), None)

        startdate = format_datetime(get_startdate(asset=asset_otl))
        if not startdate:
            logging.debug(f'Startdate cannot be determined. Skipping asset: {asset_otl.uuid}')
            continue

        # Ophalen van de bestekken van de VRLegacy.
        bestekken_lgc = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid)

        # Info tijdelijk wegschrijven
        # for idx, i in enumerate(bestekken_lgc):
        #     df_assets.at[df_index, f'bestek_{idx}_aannemernaam'] = i.bestekRef.aannemerNaam
        #     df_assets.at[df_index, f'bestek_{idx}_dossiernummer'] = i.bestekRef.eDeltaDossiernummer
        #     # debug: te veel actieve bestekken.
        #     df_assets.at[df_index, f'bestek_{idx}_status'] = i.status.value
        #     df_assets.at[df_index, f'bestek_{idx}_data'] = f'{i.startDatum} {i.eindDatum}'

        bestekken_lgc_update = copy.deepcopy(bestekken_lgc)

        # Logica toepassen in functie van de aannemer (Swarco of Yunex).
        logging.debug('Controleer of 1 van de ACTIEVE bestekken swarco of yunex is.')
        aannemers = []
        bestekkoppelingAanwezig = False
        for bestekkoppeling in bestekken_lgc_update:
            if bestekkoppeling.bestekRef.uuid in (bestekref_yunex.uuid, bestekref_swarco.uuid):
                logging.debug('Bestekkoppeling YUNEX reeds aanwezig')
                logging.debug('Herneem de startdatum van het aanwezig bestek, en stel deze later in als einddatum van de nu nog bestaande, actieve bestekken.')
                bestekkoppelingAanwezig = True
                startdate = bestekkoppeling.startDatum

            elif bestekkoppeling.status.value == 'ACTIEF':
                aannemer = bestekkoppeling.bestekRef.aannemerNaam
                logging.debug(f'De overige ACTIEVE bestekkoppeling heeft aannemer: {aannemer}')
                aannemers.append(aannemer)
                logging.debug(f'Beëindig de bestekkoppeling met als datum: {startdate}')
                bestekkoppeling.eindDatum = startdate

        if bestekkoppelingAanwezig is False:
            logging.debug('Bestekkoppeling is nog niet aanwezig, dus dient te worden aangemaakt')
            # Nieuwe bestekkoppeling instantiëren
            if [aannemer for aannemer in aannemers if 'YUNEX' in aannemer]:
                bestekkoppeling_nieuw = BestekKoppeling(bestekRef=bestekref_yunex, startDatum=startdate,
                                                        categorie=BestekCategorieEnum.WERKBESTEK, status=BestekKoppelingStatusEnum.ACTIEF)
            elif [aannemer for aannemer in aannemers if 'SWARCO' in aannemer]:
                bestekkoppeling_nieuw = BestekKoppeling(bestekRef=bestekref_swarco, startDatum=startdate,
                                                        categorie=BestekCategorieEnum.WERKBESTEK, status=BestekKoppelingStatusEnum.ACTIEF)
            else:
                raise ValueError(f'Aannemer naam moet ofwel Yunex of Swarco zijn. (={aannemer})')

            logging.debug("Nieuwe bestekkoppeling van YUNEX/Swarco toevoegen op de eerste index-positie")
            bestekken_lgc_update.insert(0, bestekkoppeling_nieuw)

        # todo: onderstaande code activeren. Opgepast voor de omgeving.
        # Alle Bestekkoppelingen updaten
        # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_lgc.uuid, bestekkoppelingen=bestekken_lgc)

    # info tijdelijk wegschrijven om de huidige bestekkoppelingen beter te kunnen analyseren.
    # df_assets.to_excel(Path().home() / 'Downloads' / 'VerkeersRegelinstallatie' / 'info_bestekkoppelingen.xlsx'
    #                    , index=False
    #                    , freeze_panes=[1, 1]
    #                    )
