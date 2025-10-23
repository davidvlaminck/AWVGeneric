import logging
import re
from typing import Any

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, TermDTO, ExpressionDTO, OperatorEnum, \
    LogicalOpEnum, AssetRelatieDTO
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report


def _get_installatienaam_from_kastnaam(kast_naam: str) -> str:
    """
    Get the rootname or installatienaam from a kast_naam.
    If the kast naam follows the regex pattern, return the rootname.
    Else, return the kast naam itself.

    :param kast_naam:
    :return:
    """
    if re.match(string=kast_naam, pattern='^.*.K\d*$'):
        return kast_naam.rsplit(sep='.K', maxsplit=1)[0]
    else:
        return kast_naam


def build_query_search_dnblaagspanning(eanNummer: str, assettype_uuid: str) -> QueryDTO:
    return QueryDTO(
        size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(
                        property='eig:b924b988-a3b4-4d0c-aa0e-c69ff503e784:a108fc8a-c522-4469-8410-62f5a0241698',
                        operator=OperatorEnum.EQ,
                        value=f"{eanNummer}")]),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='type',
                        operator=OperatorEnum.EQ,
                        value=f"{assettype_uuid}")]
                    , logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='actief',
                        operator=OperatorEnum.EQ,
                        value=True)]
                    , logicalOp=LogicalOpEnum.AND)
            ]))

def build_query_search_energiemeter(energiemeter_naam: str, assettype_uuid: str) -> QueryDTO:
    return QueryDTO(
        size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(
                        property='naam',
                        operator=OperatorEnum.EQ,
                        value=f"{energiemeter_naam}")]),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='type',
                        operator=OperatorEnum.EQ,
                        value=f"{assettype_uuid}")]
                    , logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='actief',
                        operator=OperatorEnum.EQ,
                        value=True)]
                    , logicalOp=LogicalOpEnum.AND)
            ]))

def get_first_list_element(lst: list) -> Any:
    """
    Check if the list contains multiple list elements and raise an error.
    If one list element is available, returns the first and only list element.
    :param lst:
    :return:
    """
    if len(lst) > 1:
        raise ValueError(f'{len(lst)} list elements found. Expected one.')
    return lst[0]

def create_relatie_if_missing(eminfra_client: EMInfraClient, bronAsset: AssetDTO, doelAsset: AssetDTO, relatie_uri: str) -> AssetRelatieDTO:
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
    logging.info(f'Create relatie {relatie_uri} between {bronAsset.type.korteUri} ({bronAsset.uuid}) and '
                 f'{doelAsset.type.korteUri} ({doelAsset.uuid}).')
    kenmerkTypeId, relatieTypeId = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri=relatie_uri)
    relaties = eminfra_client.search_assetrelaties(type=relatieTypeId, bronAsset=bronAsset, doelAsset=doelAsset)
    if len(relaties) > 1:
        raise ValueError(f'Found {len(relaties)}, expected 1')
    elif len(relaties) == 0:
        relatie = eminfra_client.create_assetrelatie(
            bronAsset=bronAsset,
            doelAsset=doelAsset,
            relatieType_uuid=relatieTypeId)
    elif len(relaties) == 1:
        relatie = relaties[0]
    else:
        raise NotImplementedError
    return relatie

def process_fictieve_aansluiting(eminfra_client: EMInfraClient, df: pd.DataFrame):
    logging.info("Schrap de elektrische aansluitingen A11.FICTIEF")
    for _, df_row in df.iterrows():
        asset = eminfra_client.get_asset_by_id(assettype_id=df_row["uuid"])
        logging.info(f'Schrap elektrische aansluiting voor asset: {asset.uuid}')
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset.uuid)


# sourcery skip: swap-if-else-branches, use-named-expression
if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('EAN-nummers verplaatsen:\t EAN-nummers overdragen van assets (Legacy) naar DNBLaagspanning (OTL)')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    # filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_ TEI.xlsx'
    filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_.xlsx'
    usecols = ['uuid', 'naam', 'naampad', 'isTunnel', 'toestand', 'assettype_naam', 'ean', 'aansluiting']
    df_assets = read_rsa_report(filepath, usecols=usecols)
    df_assets_fictieve_aansluiting = df_assets[df_assets['aansluiting'] == 'A11.FICTIEF']
    df_kast = df_assets[df_assets['assettype_naam'] == 'Kast (Legacy)']
    df_lsdeel = df_assets[df_assets['assettype_naam'] == 'Laagspanningsgedeelte (Legacy)']

    headers = ('uuid', 'naam', 'assettype_naam',
               'kast_uuid', 'kast_naam',
               'ls_uuid', 'ls_naam',
               'lsdeel_uuid', 'lsdeel_naam',
               'bevestiging_uuid_ls_kast', 'bevestiging_uuid_lsdeel_kast', 'voeding_ls_lsdeel',
               'dnblaagspanning_uuid', 'dnblaagspanning_naam', 'energiemeter_uuid', 'energiemeter_naam',
               'heeftBijhorendeAssets_ls_dnblaagspanning', 'heeftBijhorendeAssets_ls_energiemeter',
               'voedt_dnblaagspanning_energiemeterdnb')
    rows = []

    typeUuid_ls = '80fdf1b4-e311-4270-92ba-6367d2a42d47'
    typeUuid_lsdeel = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
    typeUuid_dnblaagspanning = 'b4ee4ea9-edd1-4093-bce1-d58918aee281'
    typeUuid_energiemeter = 'ca3ae27f-c611-4761-97d1-d9766dd30e0a'

    if not df_assets_fictieve_aansluiting.empty:
        process_fictieve_aansluiting(eminfra_client=eminfra_client, df=df_assets_fictieve_aansluiting)

    # per assettype werken om de relatie naar de kast op te sporen.
    # Kast (df_kast)
    # LSDeel (df_lsdeel)
    for idx, df_row in df_kast.iterrows():
        row = {
            "uuid": df_row["uuid"],
            "naam": df_row["naam"],
            "assettype_naam": df_row["assettype_naam"]
        }
        kast = eminfra_client.get_asset_by_id(assettype_id=df_row["uuid"])
        row["kast_uuid"] = kast.uuid
        row["kast_naam"] = kast.naam

        installatie_naam = _get_installatienaam_from_kastnaam(kast_naam=kast.naam)
        logging.info('Child-asset Laagspanning toevoegen')
        child_assets = list(eminfra_client.search_child_assets(asset_uuid=kast.uuid, recursive=False))
        ls_assets = [asset for asset in child_assets if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        if not ls_assets:
            ls_asset_naam = installatie_naam + '.LS'
            new_asset_dict = eminfra_client.create_asset(parent_uuid=kast.uuid,
                                                         typeUuid=typeUuid_ls,
                                                         naam=ls_asset_naam)
            ls = eminfra_client.get_asset_by_id(assettype_id=new_asset_dict.get("uuid"))
        else:
            ls = get_first_list_element(ls_assets)
        row["ls_uuid"] = ls.uuid
        row["ls_naam"] = ls.naam

        logging.info('Child-asset LSDeel toevoegen')
        lsdeel_assets = [asset for asset in child_assets if
                     asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
        if not lsdeel_assets:
            lsdeel_asset_naam = installatie_naam + '.LSDeel'
            new_asset_dict = eminfra_client.create_asset(parent_uuid=kast.uuid,
                                                         typeUuid=typeUuid_lsdeel,
                                                         naam=lsdeel_asset_naam)
            lsdeel = eminfra_client.get_asset_by_id(assettype_id=new_asset_dict.get("uuid"))
        else:
            lsdeel = get_first_list_element(lsdeel_assets)
        row["lsdeel_uuid"] = lsdeel.uuid
        row["lsdeel_naam"] = lsdeel.naam

        logging.info('Bevestiging-relatie LS aan Kast toevoegen')
        row["bevestiging_uuid_ls_kast"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=ls,
            doelAsset=kast,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging').uuid

        logging.info('Bevestiging-relatie LSDeel aan Kast toevoegen')
        row["bevestiging_uuid_lsdeel_kast"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=lsdeel,
            doelAsset=kast,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging').uuid

        logging.info('Voedings-relatie LS naar LSDeel toevoegen')
        row["voeding_uuid_ls_lsdeel"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=ls,
            doelAsset=lsdeel,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt').uuid

        logging.info('DNBLaagspanning toevoegen via een HoortBij-relatie naar LS')
        eanNummer = df_row["ean"]
        query_dto = build_query_search_dnblaagspanning(eanNummer=eanNummer, assettype_uuid=typeUuid_dnblaagspanning)
        dnblaagspanningen = list(eminfra_client.search_assets(query_dto=query_dto))
        if not dnblaagspanningen:
            logging.info('Create new asset DNBLaagspanning (OTL)')
            dnblaagspanning_dict = eminfra_client.create_asset_and_relatie(
                assetId=ls.uuid,
                naam=installatie_naam,
                typeUuid=typeUuid_dnblaagspanning,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            dnblaagspanning = eminfra_client.get_asset_by_id(dnblaagspanning_dict.get("uuid"))
        else:
            dnblaagspanning = get_first_list_element(dnblaagspanningen)
        row["dnblaagspanning_uuid"] = dnblaagspanning.uuid
        row["dnblaagspanning_naam"] = dnblaagspanning.naam
        row["heeftBijhorendeAssets_ls_dnblaagspanning"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=ls,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij').uuid


        logging.info('EnergiemeterDNB toevoegen via een HoortBij-relatie naar LS')
        energiemeter_naam = installatie_naam + '.ENERGIEMETER'
        query_dto = build_query_search_energiemeter(energiemeter_naam=energiemeter_naam, assettype_uuid=typeUuid_energiemeter)
        energiemeters = list(eminfra_client.search_assets(query_dto=query_dto, actief=True))
        if not energiemeters:
            logging.info('Create new asset Energiemeter (OTL)')
            energiemeter_dict = eminfra_client.create_asset_and_relatie(
                assetId=ls.uuid,
                naam=energiemeter_naam,
                typeUuid=typeUuid_energiemeter,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets')
            energiemeter = eminfra_client.get_asset_by_id(energiemeter_dict.get("uuid"))
        else:
            energiemeter = get_first_list_element(energiemeters)
        row["energiemeter_uuid"] = energiemeter.uuid
        row["energiemeter_naam"] = energiemeter.naam
        row["heeftBijhorendeAssets_ls_energiemeter"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=energiemeter,
            doelAsset=ls,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij').uuid


        logging.info('Voedt-relatie toevoegen van DNBLaagspanning naar EnergiemeterDNB')
        row["voedt_dnblaagspanning_energiemeterdnb"] = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=energiemeter,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt').uuid


        logging.info('Locatie (LSDeel en LS) afleiden via de bestaande relaties')
        eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=lsdeel.uuid, doel_asset_uuid=kast.uuid)
        eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=ls.uuid, doel_asset_uuid=kast.uuid)

        if row['voedt_dnblaagspanning_energiemeterdnb']:
            logging.info('Alle voorgaande checks zijn uitgevoerd, de nieuwe OTL-conforme manier van Elektrische '
                         'aansluiting is geïnstalleerd. Wis nu het kenmerk elektrisch aansluitpunt van de asset.')
            eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=kast.uuid)

        rows.append(row)

    logging.info('Write pandas dataframe to an Excel')
    pd.DataFrame(data=rows).to_excel('elektrische aansluitingen voorbereiding Kasten.xlsx', index=False, freeze_panes=[1,1])