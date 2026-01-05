import logging
from datetime import datetime

import pandas as pd
from pathlib import Path

from UseCases.utils import load_settings
from utils.date_helpers import format_datetime
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('V&W Vlaams-Brabant: \tBestekkoppelingen toevoegen voor een lijst van assets en deactiveren van de huidige bestekkoppeling.')

    settings_path = load_settings()
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # Read Excel as pandas dataframe
    filepath_input = Path.home() / 'OneDrive - Nordend/projects/AWV/Rapporten_overige/Bestekken/Overdragen bestekken/20251003_Installaties op verkeerd bestek.xlsx'
    usecols = ['id', 'naampad', 'type', 'actief']
    df_assets = pd.read_excel(filepath_input, sheet_name='Blad1', header=0, usecols=usecols)

    # ophalen van de eigenlijke bestekkoppeling op basis van de naam.
    bestek_naam = 'AWV/VW/2024/1_P4'
    bestekRef_new = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=bestek_naam)
    bestek_datum = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    logging.info(f'Appending bestek: {bestek_naam} with start_date: {bestek_datum}')

    for idx, df_asset in df_assets.iterrows():
        # Ophalen asset:
        asset = eminfra_client.get_asset_by_id(assettype_id=df_asset.get("id"))
        logging.debug(f'Processing asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        # search all bestekkoppelingen
        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

        # test: zijn er meerdere actieve bestekkoppelingen?
        aantal_actieve_bestekken = len([bk for bk in bestekkoppelingen if bk.status == BestekKoppelingStatusEnum.ACTIEF])
        if aantal_actieve_bestekken == 0:
            logging.debug('Er zijn geen actieve bestekkoppelingen.')
        if aantal_actieve_bestekken > 1:
            logging.debug('Er zijn meerdere actieve bestekkoppelingen.')

        # Check if the new bestekkoppeling doesn't exist and append at the end of the list, else do nothing
        if not (matching_koppeling := next(
                (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekRef_new.uuid),
                None, )):
            logging.debug(f'Bestekkoppeling "{bestekRef_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')

            logging.debug('Deactiveer alle bestekkoppelingen.')
            for bk in bestekkoppelingen:
                if bk.eindDatum is None:
                    bk.eindDatum = format_datetime(bestek_datum)

            bestekkoppeling_new = BestekKoppeling(
                bestekRef=bestekRef_new,
                status=BestekKoppelingStatusEnum.ACTIEF,
                startDatum=format_datetime(bestek_datum),
                eindDatum=None,
                categorie=BestekCategorieEnum.WERKBESTEK
            )
            # Insert the new bestekkoppeling at index position 0 (not append())
            bestekkoppelingen.insert(0, bestekkoppeling_new)

        else:
            logging.debug(f'Bestekkoppeling "{matching_koppeling.bestekRef.eDeltaDossiernummer}" bestaat al, '
                          f'status: {matching_koppeling.status.value}')

        # Update all the bestekkoppelingen for this asset
        eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)