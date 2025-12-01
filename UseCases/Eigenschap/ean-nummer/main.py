import logging
from typing import Any

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import (AssetDTO, AssetRelatieDTO, construct_naampad, RelatieEnum, EigenschapValueUpdateDTO,
                               AssetDTOToestand)
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report

TYPEUUID_LS = '80fdf1b4-e311-4270-92ba-6367d2a42d47'
TYPEUUID_HS = '46dcd9b1-f660-4c8c-8e3e-9cf794b4de75'
TYPEUUID_LSDEEL = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
TYPEUUID_HSDEEL = 'a9655f50-3de7-4c18-aa25-181c372486b1'
TYPEUUID_DNBLAAGSPANNING = 'b4ee4ea9-edd1-4093-bce1-d58918aee281'
TYPEUUID_DNBHOOGSPANNING = '8e9307e2-4dd6-4a46-a298-dd0bc8b34236'
TYPEUUID_ENERGIEMETER = 'ca3ae27f-c611-4761-97d1-d9766dd30e0a'
TYPEUUID_FORFAITAIRE_AANSLUITING = 'ffb9a236-fb9e-406f-a602-271b68e62afc'


def _get_installatienaam_from_asset(asset: AssetDTO) -> str:
    """
    Get the rootname or installatienaam from an asset.

    :param asset:
    :return:
    """
    naampad = construct_naampad(asset=asset)
    return naampad.split(sep='/')[0]

def get_first_list_element(lst: list) -> Any:
    """
    Check if the list contains multiple list elements and raise an error.
    If one list element is available, returns the first and only list element.
    :param lst:
    :return:
    """
    if not lst:
        raise ValueError('Input is an empty list.')
    elif len(lst) > 1:
        raise ValueError(f'{len(lst)} list elements found. Expected one.')
    return lst[0]


def create_relatie_if_missing(eminfra_client: EMInfraClient, bronAsset: AssetDTO, doelAsset: AssetDTO,
                              relatie: RelatieEnum) -> AssetRelatieDTO:
    """
    Given a relatie type (relatie_uri), and two assets (bronAsset, doelAsset), search for the existing relation(s)
    and create a new relation if missing.
    Raise an error if multiple relations exist.
    Returns the object AssetRelatieDTO.

    :param eminfra_client:
    :param bronAsset:
    :param doelAsset:
    :param relatie_uri:
    :return:
    """
    logging.info(f'Create relatie {relatie.value} between {bronAsset.type.korteUri} ({bronAsset.uuid}) and '
                 f'{doelAsset.type.korteUri} ({doelAsset.uuid}).')
    kenmerkTypeId, relatieTypeId = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie=relatie)
    relaties = eminfra_client.search_assetrelaties(type=relatieTypeId, bronAsset=bronAsset, doelAsset=doelAsset)
    if len(relaties) > 1:
        raise ValueError(f'Found {len(relaties)}, expected 1')
    elif len(relaties) == 0:
        relatie_output = eminfra_client.create_assetrelatie(bronAsset=bronAsset, doelAsset=doelAsset, relatie=relatie)
    elif len(relaties) == 1:
        relatie_output = relaties[0]
    else:
        raise NotImplementedError
    return relatie_output


def transfer_ean_number(eminfra_client, bronAsset, doelAsset, ean_bronAsset):
    """
    Transfers the ean-number from bronAsset to doelAsset.
    Typically, the ean-number is documented as "elektrisch aansluitpunt" for a bronAsset (Laagspanning)
    and transferred to property "ean nummer" at doelAsset (DNBLaagspanning).

    :param eminfra_client:
    :param bronAsset:
    :param doelAsset:
    :param ean_bronAsset: 18 digit EAN-number
    :return:
    """
    logging.info('Toevoegen EAN-nummer als eigenschap')
    eigenschap_eanNummer_list = eminfra_client.get_eigenschappen(assetId=doelAsset.uuid,
                                                                 eigenschap_naam='eanNummer')
    if eigenschap_eanNummer_list:
        eigenschap_eanNummer = get_first_list_element(eigenschap_eanNummer_list)
        ean_doelAsset = eigenschap_eanNummer.typedValue["value"]
        if ean_bronAsset == ean_doelAsset:
            logging.info('Identiek ean-nummer. Geen updates')
            action = "Loskoppelen eigenschap elektrisch aansluitpunt."
        else:
            log_message = (f'\n{bronAsset.uuid}'
                           f'\nLaagspanning eigenschap "elektrische aansluiting": {ean_bronAsset}.'
                           f'\nDNBLaagspanning eigenschap "ean nummer": {eigenschap_eanNummer.typedValue["value"]}'
                           f'\nConflicterende waarden!')
            logging.critical(log_message)
            # raise ValueError(log_message)
            action = None
    else:
        logging.info('Eigenschap eanNummer bij DNBLaagspanning is nog onbestaande. Ken waarde toe.')
        action = "Loskoppelen eigenschap elektrisch aansluitpunt."
        eigenschapValueUpdate_eanNummer = EigenschapValueUpdateDTO(
            eigenschap=get_first_list_element(
                eminfra_client.search_eigenschappen(
                    eigenschap_naam='eanNummer'
                    , uri='https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#DNB.eanNummer'))
            , typedValue={'_type': 'text', 'value': f'{ean_bronAsset}'}
        )
        eminfra_client.update_eigenschap(assetId=doelAsset.uuid, eigenschap=eigenschapValueUpdate_eanNummer)

    if action:
        logging.info(action)
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=bronAsset.uuid)

def process_fictieve_aansluiting(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Loskoppelen fictieve aansluitingen
    Loskoppelen elektrische aansluiting voor alle assets in een dataframe.
    :param eminfra_client:
    :param df:
    :return:
    """
    logging.info("Schrap de elektrische aansluitingen A11.FICTIEF")
    for _, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Schrap elektrische aansluiting voor asset: {asset.uuid}')
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)


def process_seinbrug(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Het nummer van de elektrische aansluiting (EAN) toevoegen als commentaar en de elektrische aansluiting zelf loskoppelen.
    :param eminfra_client:
    :param df:
    :return:
    """
    df.reset_index(drop="index", inplace=True)
    for idx, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Processing asset ({idx + 1}/{len(df)}): {asset.uuid}')
        elektrischAansluitpuntKenmerk = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)
        if elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef is None:
            logging.info('Asset bevat geen kenmerk elektrische aansluiting meer. Op naar de volgende.')
            continue
        else:
            eanNummer = elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef.get('ean')

        if commentaar := asset.commentaar:
            commentaar += f'; EAN: {eanNummer}'
        else:
            commentaar = f'EAN: {eanNummer}'

        eminfra_client.update_commentaar(asset=asset, commentaar=commentaar)
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)


def process_forfait(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Toevoegen van twee assets via de HoortBij-relatie: DNBLaagspanning en Forfaitaire aansluiting.
    Toevoegen Voedt-relatie van DNBLaagspanning naar Forfaitaire aansluiting
    Loskoppelen van de Elektrische aansluiting

    :param eminfra_client:
    :param df:
    :return:
    """
    df.reset_index(drop="index", inplace=True)
    for idx, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Processing asset ({idx+1}/{len(df)}): {asset.uuid}')
        installatie_naam = _get_installatienaam_from_asset(asset=asset)
        elektrischAansluitpuntKenmerk = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)
        if elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef is None:
            logging.info('Asset bevat geen kenmerk elektrische aansluiting meer. Op naar de volgende.')
            continue
        else:
            ean_bronAsset = elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef.get('ean')

        logging.info('Assets ophalen via het kenmerk "HeeftBijhorendeAssets"')
        assets_hoortBij = eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid,
                                                                   relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)

        dnblaagspanning_assets = [asset for asset in assets_hoortBij if
                                  asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning']
        forfaitaire_aansluiting_assets = [asset for asset in assets_hoortBij if
                               asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting']

        logging.info('DNBLaagspanning toevoegen via een HoortBij-relatie')
        if not dnblaagspanning_assets:
            logging.info('Create new asset DNBLaagspanning (OTL)')
            dnblaagspanning_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=installatie_naam,
                typeUuid=TYPEUUID_DNBLAAGSPANNING,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            dnblaagspanning = eminfra_client.get_asset_by_id(dnblaagspanning_dict.get("uuid"))
        else:
            dnblaagspanning = get_first_list_element(dnblaagspanning_assets)
        eminfra_client.update_toestand(asset=dnblaagspanning, toestand=AssetDTOToestand.IN_GEBRUIK)

        logging.info('Enkel de elektrische aansluiting ontkoppelend, '
                     'maar het forfaitaire ean-nummer niet overdragen naar DNBLaagspanning')
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)

        logging.info('Forfaitaire aansluiting toevoegen via een HoortBij-relatie')
        if not forfaitaire_aansluiting_assets:
            logging.info('Create new asset Forfaitaire Aansluiting (OTL)')
            forfaitaire_aansluiting_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=f'{installatie_naam}.FORFAIT',
                typeUuid=TYPEUUID_FORFAITAIRE_AANSLUITING,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            forfaitaire_aansluiting = eminfra_client.get_asset_by_id(forfaitaire_aansluiting_dict.get("uuid"))
        else:
            forfaitaire_aansluiting = get_first_list_element(forfaitaire_aansluiting_assets)
        eminfra_client.update_toestand(asset=forfaitaire_aansluiting, toestand=AssetDTOToestand.IN_GEBRUIK)

        logging.info('Voedt-relatie toevoegen tussen dnblaagspanning en forfaitaire aansluiting')
        create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=forfaitaire_aansluiting,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')


def process_laagspanning(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Toevoegen van twee assets via de HoortBij-relatie: DNBLaagspanning en Energiemeter.
    Toevoegen Voedt-relatie van DNBLaagspanning naar Energiemeter
    Loskoppelen van de Elektrische aansluiting

    :param eminfra_client:
    :param df:
    :return:
    """
    df.reset_index(drop="index", inplace=True)
    for idx, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Processing asset ({idx+1}/{len(df)}): {asset.uuid}')
        installatie_naam = _get_installatienaam_from_asset(asset=asset)
        elektrischAansluitpuntKenmerk = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)
        if elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef is None:
            logging.info('Asset bevat geen kenmerk elektrische aansluiting meer. Op naar de volgende.')
            continue
        else:
            ean_bronAsset = elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef.get('ean')

        logging.info('Assets ophalen via het kenmerk "HeeftBijhorendeAssets"')
        assets_hoortBij = eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid,
                                                                   relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)

        dnblaagspanning_assets = [asset for asset in assets_hoortBij if
                                  asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning']
        energiemeter_assets = [asset for asset in assets_hoortBij if
                               asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']

        logging.info('DNBLaagspanning toevoegen via een HoortBij-relatie')
        if not dnblaagspanning_assets:
            logging.info('Create new asset DNBLaagspanning (OTL)')
            dnblaagspanning_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=installatie_naam,
                typeUuid=TYPEUUID_DNBLAAGSPANNING,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            dnblaagspanning = eminfra_client.get_asset_by_id(dnblaagspanning_dict.get("uuid"))
        else:
            dnblaagspanning = get_first_list_element(dnblaagspanning_assets)
        eminfra_client.update_toestand(asset=dnblaagspanning, toestand=AssetDTOToestand.IN_GEBRUIK)

        transfer_ean_number(eminfra_client, asset, dnblaagspanning, ean_bronAsset)

        logging.info('Energiemeter toevoegen via een HoortBij-relatie')
        if not energiemeter_assets:
            logging.info('Create new asset Energiemeter (OTL)')
            energiemeter_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=f'{installatie_naam}.ENERGIEMETER',
                typeUuid=TYPEUUID_ENERGIEMETER,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            energiemeter = eminfra_client.get_asset_by_id(energiemeter_dict.get("uuid"))
        else:
            energiemeter = get_first_list_element(energiemeter_assets)
        eminfra_client.update_toestand(asset=energiemeter, toestand=AssetDTOToestand.IN_GEBRUIK)

        logging.info('Voedt-relatie toevoegen tussen dnblaagspanning en energiemeter')
        create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=energiemeter,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')


def process_hoogspanning(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Toevoegen van 1 assets via de HoortBij-relatie: DNBHoogspanning.
    Overzetten van de Elektrische aansluiting van Hoogspanning (Legacy) naar DNBHoogspanning (OTL)

    :param eminfra_client:
    :param df:
    :return:
    """
    df.reset_index(drop="index", inplace=True)
    for idx, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Processing asset ({idx+1}/{len(df)}): {asset.uuid}')
        installatie_naam = _get_installatienaam_from_asset(asset=asset)
        elektrischAansluitpuntKenmerk = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)
        if elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef is None:
            logging.info('Asset bevat geen kenmerk elektrische aansluiting meer. Op naar de volgende.')
            continue
        else:
            ean_bronAsset = elektrischAansluitpuntKenmerk.elektriciteitsAansluitingRef.get('ean')

        logging.info('Assets ophalen via het kenmerk "HeeftBijhorendeAssets"')
        assets_hoortBij = eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid,
                                                                   relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)

        dnbhoogspanning_assets = [asset for asset in assets_hoortBij if
                                  asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning']
        energiemeter_assets = [asset for asset in assets_hoortBij if
                               asset.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']

        logging.info('DNBHoogspanning toevoegen via een HoortBij-relatie')
        if not dnbhoogspanning_assets:
            logging.info('Create new asset DNBHoogspanning (OTL)')
            dnbhoogspanning_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=installatie_naam,
                typeUuid=TYPEUUID_DNBHOOGSPANNING,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            dnbhoogspanning = eminfra_client.get_asset_by_id(dnbhoogspanning_dict.get("uuid"))
        else:
            dnbhoogspanning = get_first_list_element(dnbhoogspanning_assets)
        eminfra_client.update_toestand(asset=dnbhoogspanning, toestand=AssetDTOToestand.IN_GEBRUIK)

        transfer_ean_number(eminfra_client, asset, dnbhoogspanning, ean_bronAsset)

        logging.info('Energiemeter toevoegen via een HoortBij-relatie')
        if not energiemeter_assets:
            logging.info('Create new asset Energiemeter (OTL)')
            energiemeter_dict = eminfra_client.create_asset_and_relatie(
                assetId=asset.uuid,
                naam=f'{installatie_naam}.ENERGIEMETER',
                typeUuid=TYPEUUID_ENERGIEMETER,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            energiemeter = eminfra_client.get_asset_by_id(energiemeter_dict.get("uuid"))
        else:
            energiemeter = get_first_list_element(energiemeter_assets)
        eminfra_client.update_toestand(asset=energiemeter, toestand=AssetDTOToestand.IN_GEBRUIK)


def process_tunnel(eminfra_client: EMInfraClient, df: pd.DataFrame):
    """
    Loskoppelen van alle elektrische aansluitingen

    :param eminfra_client:
    :param df:
    :return:
    """
    df.reset_index(drop="index", inplace=True)
    for idx, df_row in df.iterrows():
        asset = list(eminfra_client.search_asset_by_uuid(asset_uuid=df_row["uuid"]))[0]
        logging.info(f'Processing asset ({idx + 1}/{len(df)}): {asset.uuid}')
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info('EAN-nummers verplaatsen:\t EAN-nummers overdragen van assets (Legacy) naar DNBLaagspanning (OTL)')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_.xlsx'
    usecols = ['uuid', 'naam', 'naampad', 'isTunnel', 'toestand', 'assettype_naam', 'ean', 'aansluiting',
               'opmerkingen (blijvend)']
    df_assets = read_rsa_report(filepath, usecols=usecols)
    df_assets_fictieve_aansluiting = df_assets[df_assets['aansluiting'] == 'A11.FICTIEF']
    df_seinbrug = df_assets[df_assets['opmerkingen (blijvend)'] == 'Operatie toevoegen als commentaar']
    df_biflash = df_assets[df_assets['opmerkingen (blijvend)'] == 'Operatie Bi-flash forfait']
    df_forfait = df_assets[df_assets['opmerkingen (blijvend)'] == 'FORFAIT']
    df_laagspanning = df_assets[df_assets['opmerkingen (blijvend)'] == 'Operatie Laagspanning']
    df_hoogspanning = df_assets[df_assets['opmerkingen (blijvend)'] == 'Operatie Hoogspanning']
    df_beverentunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Beverentunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_craeybeckxtunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Craeybeckxtunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_craeybeckxtunnel_hs = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Craeybeckxtunnel') & (df_assets['assettype_naam'] == 'Hoogspanning (Legacy)')]
    df_jandevostunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Jan de Vos tunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_kennedytunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Kennedytunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_krijgsbaantunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Krijgsbaantunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_leonardtunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Leonardtunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_noordzuidtunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Noord-Zuid tunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]
    df_tijsmanstunnel = df_assets[(df_assets['opmerkingen (blijvend)'] == 'Tijsmanstunnel') & (df_assets['assettype_naam'] != 'Hoogspanning (Legacy)')]


    headers = ('uuid', 'naam', 'assettype_naam',
               'kast_uuid', 'kast_naam',
               'ls_uuid', 'ls_naam',
               'lsdeel_uuid', 'lsdeel_naam',
               'bevestiging_uuid_ls_kast', 'bevestiging_uuid_lsdeel_kast', 'voeding_ls_lsdeel',
               'dnblaagspanning_uuid', 'dnblaagspanning_naam', 'energiemeter_uuid', 'energiemeter_naam',
               'heeftBijhorendeAssets_ls_dnblaagspanning', 'heeftBijhorendeAssets_ls_energiemeter',
               'voedt_dnblaagspanning_energiemeterdnb')

    logging.info("Loskoppelen fictieve aansluitingen")
    if not df_assets_fictieve_aansluiting.empty:
        process_fictieve_aansluiting(eminfra_client=eminfra_client, df=df_assets_fictieve_aansluiting)

    logging.info("Seinbrug: Loskoppelen elektrische aansluiting en toevoegen van EAN-nummer als commentaar.")
    if not df_seinbrug.empty:
        process_seinbrug(eminfra_client, df_seinbrug)

    logging.info("Bi-flash: toevoegen van een bijhorende DNBLaagspanning (OTL) en forfaitaire aansluiting (OTL).")
    if not df_biflash.empty:
        process_forfait(eminfra_client, df_biflash)

    logging.info("forfait: toevoegen van bijhorende assets conform forfaitaire aansluitingen (DNBLaagspanning (OTL) en forfaitaire aansluiting (OTL)).")
    if not df_forfait.empty:
        process_forfait(eminfra_client, df_forfait)

    logging.info("Laagspanning: toevoegen van een bijhorende DNBLaagspanning (OTL) en Energiemeter (OTL).")
    if not df_laagspanning.empty:
        process_laagspanning(eminfra_client, df_laagspanning)

    logging.info("Hoogspanning: toevoegen van een bijhorende DNBHoogspanning (OTL) en Energiemeter (OTL).")
    if not df_hoogspanning.empty:
        process_hoogspanning(eminfra_client, df_hoogspanning)

    if not df_craeybeckxtunnel_hs.empty:
        process_hoogspanning(eminfra_client, df_craeybeckxtunnel_hs)

    logging.info("Elektrische aansluitingen loskoppelen voor alle tunnel elementen")
    if not df_beverentunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_beverentunnel)
    if not df_craeybeckxtunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_craeybeckxtunnel)
    if not df_jandevostunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_jandevostunnel)
    if not df_kennedytunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_kennedytunnel)
    if not df_krijgsbaantunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_krijgsbaantunnel)
    if not df_leonardtunnel.empty:
        process_tunnel(eminfra_client=eminfra_client,df=df_leonardtunnel)
    if not df_noordzuidtunnel.empty:
        process_tunnel(eminfra_client=eminfra_client,df=df_noordzuidtunnel)
    if not df_tijsmanstunnel.empty:
        process_tunnel(eminfra_client=eminfra_client, df=df_tijsmanstunnel)
