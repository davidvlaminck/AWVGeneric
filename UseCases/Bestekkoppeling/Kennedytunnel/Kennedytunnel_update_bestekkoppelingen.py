import logging
from datetime import datetime

import pandas as pd
from pathlib import Path

from utils.date_helpers import format_datetime
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Tunnelorganisatie Vlaanderen: \tBestekkoppelingen toevoegen voor assets van de Kennedytunnel')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # Read Excel as pandas dataframe
    filepath_input = Path.home() / 'Downloads' / 'Bestekken_tunnels' / 'Kennedytunnel' / 'export_Bestekken-KEN.TUNNEL.xlsx'
    usecols = ['id', 'naampad', '1e bestek', 'Startdatum 1e bestek', '2e bestek', 'Startdatum 2e bestek', '3e bestek', 'Startdatum 3e bestek']
    df_assets = pd.read_excel(filepath_input, sheet_name='Sheet0', header=0, usecols=usecols)
    df_assets.rename(columns={'id': 'uuid', '1e bestek': 'bestek1_naam', 'Startdatum 1e bestek': 'bestek1_datum', '2e bestek': 'bestek2_naam', 'Startdatum 2e bestek': 'bestek2_datum', '3e bestek': 'bestek3_naam', 'Startdatum 3e bestek': 'bestek3_datum'}, inplace=True)

    for idx, df_asset in df_assets.iterrows():
        """
        df_asset is de record van het dataframe
        asset is het AssetDTO object
        """
        # Ophalen asset:
        asset = eminfra_client.get_asset_by_id(asset_id=df_asset.get("uuid"))
        logging.debug(f'Processing asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        # search all bestekkoppelingen
        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

        # ophalen van de huidige/bestaande/actuele bestekkoppeling op basis van de naam.
        for i in range(1,4):
            bestek_naam = df_asset[f"bestek{i}_naam"]
            logging.debug('Skip empty values of Bestekken')
            if pd.isna(bestek_naam):
                continue
            bestek_datum = datetime.strptime(df_asset[f"bestek{i}_datum"], "%d/%m/%Y %H:%M")
            logging.info(f'Appending bestek: {bestek_naam} with start_date: {bestek_datum}')

            bestekRef_new = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=bestek_naam)

            # Check if the new bestekkoppeling doesn't exist and append at the end of the list, else do nothing
            if not (matching_koppeling := next(
                    (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekRef_new.uuid),
                    None, )):
                logging.debug(f'Bestekkoppeling "{bestekRef_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')
                bestekkoppeling_new = BestekKoppeling(
                    bestekRef=bestekRef_new,
                    status=BestekKoppelingStatusEnum.ACTIEF,
                    startDatum=format_datetime(bestek_datum),
                    eindDatum=None,
                    categorie=BestekCategorieEnum.WERKBESTEK
                )
                # Insert the new bestekkoppeling at the end (not specifically in front at index position 0)
                bestekkoppelingen.append(bestekkoppeling_new)

            else:
                logging.debug(f'Bestekkoppeling "{matching_koppeling.bestekRef.eDeltaDossiernummer}" bestaat al, '
                              f'status: {matching_koppeling.status.value}')

        # Herorden de volgorde van de bestekkoppelingen: alle inactieve onderaan de lijst.
        bestekkoppelingen = sorted(bestekkoppelingen, key=lambda x: x.status.value, reverse=False)

        # Update all the bestekkoppelingen for this asset
        eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)