import logging
from pathlib import Path
import re

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import ToezichtKenmerkUpdateDTO, ResourceRefDTO, BeheerobjectDTO, AssetDTO

from UseCases.utils import load_settings, read_rsa_report, configure_logger


def map_toezichtgroep(client: EMInfraClient, beheerobject_naam: str) -> ResourceRefDTO | None:
    """
    Map een beheerobject naam naar de uuid van een toezichtgroep

    Extraheer de eerst voorkomende letter uit de naam van een beheerobject.
    Map deze letter naar een toezichtsgroep.
    Zoek vervolgens de toezichtgroepDTO en geef de uuid terug.
    Return None indien geen letter kon geëxtraheerd worden of indien de letter niet overeenstemt met een toezichtsgroep.
    Deze letters stemmen overeen met de 5 Vlaamse provincies (WW, OW, A, C, G)

    :param client:
    :type client: EMInfraClient
    :param beheerobject_naam: naam van het beheerobject
    :type beheerobject_naam: str
    :return: ResourceRefDTO
    """
    regex_pattern = '(WW)|(WO)|(A)|(C)|(G)'
    letter = (m := re.search(regex_pattern, beheerobject_naam)) and m[0]

    dict_toezichtsgroep = {
        "WW": "V&W-WW",
        "WO": "V&W-WO",
        "A": "V&W-WA",
        "C": "V&W-WVB",
        "G": "V&W-WL"
    }
    if toezichtgroep_naam := dict_toezichtsgroep.get(letter.upper()):
        if toezichtgroep := next(
                client.toezichter_service.search_toezichtgroep_lgc(
                    naam=toezichtgroep_naam
                ),
                None,
        ):
            return ResourceRefDTO(uuid=toezichtgroep.uuid, links=toezichtgroep.links)


def validate_parent_asset(asset: AssetDTO) -> bool:
    """
    Validate is a parent asset is valid in this scope. Meaning a legact asset and not an installatie/beheerobject.
    :return: boolean
    """
    if isinstance(asset, BeheerobjectDTO):
        logging.debug("Parent asset is Beheerobject / Installatie.")
        return False
    elif asset.type.uri.startswith('https://wegenenverkeer.data.vlaanderen.be'):
        logging.debug(f'Parent asset type is OTL. Skip this asset ({asset.uuid}) for now.')
        return False
    else:
        return True


def process_assets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process assets from a dataframe.

    Update de toezichter en de Toezichtsgroep.
    1. Zoek de toezichter en de toezichtsgroep van de rechtstreekse parent-asset
    2. Bepaal de toezichtsgroep op basis van de letter uit de naam van het beheerobject (WW, WO, A, C, G)
    Geeft een (ander) dataframe terug dat kan gebruikt worden om een overzicht van de updates naar bijvoorbeeld
    een Excel-bestand weg te schrijven.
    """
    rows = []
    # loop over the dataframe assets
    for idx, df_row in df.iterrows():
        asset_uuid = df_row["uuid"]
        logging.info(f'Processing asset ({idx + 1}/{len(df)}): ({asset_uuid} - {df_row["naampad"]})')

        # toezichter en toezichtsgroep ophalen
        # toezichter en toezichtsgroep zitten in het object toezichterkenmerk vervat.
        asset_toezichterkenmerk = eminfra_client.toezichter_service.get_toezichter_by_uuid(asset_uuid=asset_uuid)

        # parent-asset ophalen
        parent_asset = eminfra_client.asset_service.search_parent_asset_by_uuid(
            asset_uuid=asset_uuid, recursive=False, return_all_parents=False)
        if not validate_parent_asset(parent_asset):
            logging.debug('No further processing steps')
            continue

        # toezichter en toezichtsgroep van de parent-asset ophalen
        parent_asset_toezichterkenmerk = eminfra_client.toezichter_service.get_toezichter_by_uuid(
            asset_uuid=parent_asset.uuid)

        # beheerobject ophalen
        beheerobject = eminfra_client.asset_service.search_parent_asset_by_uuid(
            asset_uuid=asset_uuid, recursive=True, return_all_parents=False)

        # update toezichter: eerst asset, nadien parent-asset
        toezichtkenmerkupdate = ToezichtKenmerkUpdateDTO(
            toezichter=asset_toezichterkenmerk.toezichter,
            toezichtGroep=asset_toezichterkenmerk.toezichtGroep
        )
        if not asset_toezichterkenmerk.toezichter and parent_asset_toezichterkenmerk.toezichter:
            toezichtkenmerkupdate.toezichter = parent_asset_toezichterkenmerk.toezichter

        # update toezichtgroep: eerst asset, nadien parent-asset, nadien via mapping van de letter.
        if not asset_toezichterkenmerk.toezichtGroep:
            if parent_asset_toezichterkenmerk.toezichtGroep:
                toezichtkenmerkupdate.toezichtGroep = parent_asset_toezichterkenmerk.toezichtGroep
            else:
                toezichtkenmerkupdate.toezichtGroep = map_toezichtgroep(
                    client=eminfra_client, beheerobject_naam=beheerobject.naam)

        row = {"asset_uuid": asset_uuid}
        if asset_toezichterkenmerk.toezichter:
            row["toezichter_oud_uuid"] = asset_toezichterkenmerk.toezichter.uuid
            row["toezichter_oud_naam"] = eminfra_client.toezichter_service.get_identiteit(
                toezichter_uuid=asset_toezichterkenmerk.toezichter.uuid).naam
        if asset_toezichterkenmerk.toezichtGroep:
            row["toezichtgroep_oud_uuid"] = asset_toezichterkenmerk.toezichtGroep.uuid
            row["toezichtgroep_oud_naam"] = eminfra_client.toezichter_service.get_toezichtgroep(
                toezichtgroep_uuid=asset_toezichterkenmerk.toezichtGroep.uuid).naam
        if toezichtkenmerkupdate.toezichter:
            row["toezichter_nieuw_uuid"] = toezichtkenmerkupdate.toezichter.uuid
            row["toezichter_nieuw_naam"] = eminfra_client.toezichter_service.get_identiteit(
                toezichter_uuid=toezichtkenmerkupdate.toezichter.uuid).naam
        if toezichtkenmerkupdate.toezichtGroep:
            row["toezichtgroep_nieuw_uuid"] = toezichtkenmerkupdate.toezichtGroep.uuid
            row["toezichtgroep_nieuw_naam"] = eminfra_client.toezichter_service.get_toezichtgroep(
                toezichtgroep_uuid=toezichtkenmerkupdate.toezichtGroep.uuid).naam
        rows.append(row)

        eminfra_client.toezichter_service.update_toezichtkenmerk(
            asset_uuid=asset_uuid,
            toezichtkenmerkupdate=toezichtkenmerkupdate
        )

    return pd.DataFrame(rows)


if __name__ == '__main__':
    configure_logger()
    logging.info('https://github.com/davidvlaminck/AWVGeneric/issues/179')
    logging.info('toezichter en toezichtsgroep toekennen')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    input_file = (Path.home() / 'Nordend' / 'AWV - Documents' / 'reportingServiceAssets' / 'Report0217' /
                  '[RSA] Toezichtsgroep ontbreekt voor voeding-assets (LS, LSDeel, HS, HSDeel, HSCabine).xlsx')
    df_assets = read_rsa_report(filepath=input_file,
                                usecols=['uuid', 'assettype', 'toestand', 'naampad', 'naam', 'toezichter_naam',
                                         'opmerkingen (blijvend)'])

    # filter df op toestand en op lege commentaarvelden
    df_assets = df_assets[
        df_assets["toestand"].isin(['gepland', 'in-gebruik', 'in-ontwerp', 'in-opbouw']) &
        df_assets['opmerkingen (blijvend)'].isna()
        ]
    df_assets.reset_index(inplace=True)

    df_output = process_assets(df=df_assets)

    output_excel_path = 'test_output.xlsx'
    if Path(output_excel_path).exists():
        # Append to existing file
        with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df_output.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])
    else:
        # Write to a new file
        with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
            df_output.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])
