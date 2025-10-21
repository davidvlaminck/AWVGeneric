import logging
import re

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, TermDTO, ExpressionDTO, OperatorEnum, \
    LogicalOpEnum
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
    regex_match = re.match(string=kast_naam, pattern='^.*.K\d*$')
    if regex_match:
        return kast_naam.rsplit(sep='.K', maxsplit=1)[0]
    else:
        return kast_naam


def search_relaties(eminfra_client: EMInfraClient, bronAsset: AssetDTO, doelAsset: AssetDTO, relatie_uri: str):
    kenmerkTypeId, relatieTypeId = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri=relatie_uri)
    relaties = list(
        eminfra_client.search_relaties(assetId=bronAsset.uuid, relatieTypeId=relatieTypeId,
                                       kenmerkTypeId=kenmerkTypeId))
    return [rel for rel in relaties if rel.type.get('uri') == doelAsset.type.uri]

def create_relatie_if_missing(eminfra_client: EMInfraClient, bronAsset: AssetDTO, doelAsset: AssetDTO, relatie_uri: str) -> [str]:
    """
    Create a list of relatie_uuid's between two assets, given a specific relatie_uri.

    :param eminfra_client:
    :param bronAsset:
    :param doelAsset:
    :param relatie_uri:
    :return:
    """
    logging.info(f'Create relatie {relatie_uri} between {bronAsset.type} ({bronAsset.uuid}) and {doelAsset.type} ({doelAsset.uuid}).')
    kenmerkTypeId, relatieTypeId = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri=relatie_uri)
    relaties = list(
        eminfra_client.search_relaties(assetId=bronAsset.uuid, relatieTypeId=relatieTypeId,
                                       kenmerkTypeId=kenmerkTypeId))
    relaties = [rel for rel in relaties if rel.type.get('uri') == doelAsset.type.uri]
    relaties = search_relaties()
    if not relaties:
        # No relaties → create one
        return [
            eminfra_client.create_assetrelatie(
                bronAsset_uuid=bronAsset.uuid,
                doelAsset_uuid=doelAsset.uuid,
                relatieType_uuid=relatieTypeId
            )
        ]
        # One or more relaties → just collect their UUIDs
    return [rel.uuid for rel in relaties]


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('EAN-nummers verplaatsen:\t EAN-nummers overdragen van assets (Legacy) naar DNBLaagspanning (OTL)')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_ TEI.xlsx'
    usecols = ['uuid', 'naam', 'naampad', 'isTunnel', 'toestand', 'assettype_naam', 'ean', 'aansluiting']
    df_assets = read_rsa_report(filepath, usecols=usecols)
    df_kast = df_assets[df_assets['assettype_naam'] == 'Kast (Legacy)']
    df_lsdeel = df_assets[df_assets['assettype_naam'] == 'Laagspanningsgedeelte (Legacy)']

    headers = ('assettype', 'asset_uuid', 'asset_naam',
               'kast_uuid', 'kast_naam',
               'ls_uuid', 'ls_naam',
               'lsdeel_uuid', 'lsdeel_naam',
               'bevestiging_uuid_ls_kast', 'bevestiging_uuid_lsdeel_kast', 'voeding_ls_lsdeel',
               'dnblaagspanning_uuid', 'dnblaagspanning_naam', 'energiemeter_uuid', 'energiemeter_naam',
               'hoortbij_dnblaagspanning_ls', 'hoortbij_energiemeter_ls', 'voedt_dnblaagspanning_energiemeterdnb')
    rows = []

    # todo controleren dat de eanNummers niet nog elders gebruikt worden.

    for idx, df_row in df_kast.iterrows():
        # logging.info('Naamconventie Kast toepassen: XXXX.K')
        kast = eminfra_client.get_asset_by_id(assettype_id=df_row["uuid"])
        row = {
            "kast_uuid": kast.uuid,
            "kast_naam": kast.naam}



        logging.info('Child-asset Laagspanning toevoegen XXXX.K.LS')
        child_assets = list(eminfra_client.search_child_assets(asset_uuid=kast.uuid, recursive=False))
        ls_assets = [asset for asset in child_assets if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        if len(ls_assets) == 0:
            ls_asset_naam = _get_installatienaam_from_kastnaam(kast_naam=kast.naam) + '.LS'
            new_asset_dict = eminfra_client.create_asset(parent_uuid=kast.uuid,
                                                         typeUuid='80fdf1b4-e311-4270-92ba-6367d2a42d47',
                                                         naam=ls_asset_naam)
            ls = eminfra_client.get_asset_by_id(assettype_id=new_asset_dict.get("uuid"))
        elif len(ls_assets) > 1:
            logging.error("Multiple ls found, skip to the next asset")
            continue
        else:
            ls = ls_assets[0]
        row["ls_uuid"] = ls.uuid
        row["ls_naam"] = ls.naam


        logging.info('Child-asset LSDeel toevoegen XXXX.K.LSDeel')
        lsdeel_assets = [asset for asset in child_assets if
                     asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
        if len(lsdeel_assets) == 0:
            lsdeel_asset_naam = _get_installatienaam_from_kastnaam(kast_naam=kast.naam) + '.LSDeel'
            new_asset_dict = eminfra_client.create_asset(parent_uuid=kast.uuid,
                                                         typeUuid='b4361a72-e1d5-41c5-bfcc-d48f459f4048',
                                                         naam=lsdeel_asset_naam)
            lsdeel = eminfra_client.get_asset_by_id(assettype_id=new_asset_dict.get("uuid"))
        elif len(lsdeel_assets) > 1:
            logging.error("Multiple lsdeel found, skip to the next asset")
            continue
        else:
            lsdeel = lsdeel_assets[0]
        row["lsdeel_uuid"] = lsdeel.uuid
        row["lsdeel_naam"] = lsdeel.naam

        logging.info('Bevestiging-relatie LS aan Kast toevoegen')
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=kast,
            doelAsset=ls,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["bevestiging_uuid_ls_kast"] = relaties[0]

        logging.info('Bevestiging-relatie LSDeel aan Kast toevoegen')
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=kast,
            doelAsset=lsdeel,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["bevestiging_uuid_lsdeel_kast"] = relaties[0]

        logging.info('Voedings-relatie LS naar LSDeel toevoegen')
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=ls,
            doelAsset=lsdeel,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["voeding_uuid_ls_lsdeel"] = relaties[0]


        logging.info('DNBLaagspanning toevoegen via een HoortBij-relatie naar LS')
        # eerst DNBLaagspanning toevoegen. Naam is de installatie naam en eigenschap eanNummer
        dnblaagspanning_naam = _get_installatienaam_from_kastnaam(kast_naam=kast.naam)
        eanNummer = df_row["ean"]
        typeUuid_dnblaagspanning = 'b4ee4ea9-edd1-4093-bce1-d58918aee281'
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(
                            property='eig:b924b988-a3b4-4d0c-aa0e-c69ff503e784:a108fc8a-c522-4469-8410-62f5a0241698',
                            operator=OperatorEnum.EQ,
                            value=eanNummer)]),
                    ExpressionDTO(
                        terms=[TermDTO(
                            property='type',
                            operator=OperatorEnum.EQ,
                            value=typeUuid_dnblaagspanning,
                            logicalOp=LogicalOpEnum.AND)])
                ]))
        dnblaagspanning = next(eminfra_client.search_assets(query_dto=query_dto, actief=True), None)
        if not dnblaagspanning:
            logging.info('Create new asset DNBLaagspanning (OTL)')
            dnblaagspanning_dict = eminfra_client.create_asset_and_relatie(
                assetId=ls.uuid,
                naam=dnblaagspanning_naam,
                typeUuid=typeUuid_dnblaagspanning,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')
            dnblaagspanning = eminfra_client.get_asset_by_id(dnblaagspanning_dict.get("uuid"))
        row["dnblaagspanning_uuid"] = dnblaagspanning.uuid
        row["dnblaagspanning_naam"] = dnblaagspanning.naam
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=ls,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["hoortbij_dnblaagspanning_ls"] = relaties[0]


        logging.info('EnergiemeterDNB toevoegen via een HoortBij-relatie naar LS')
        # eerst Energiemeter toevoegen. Naam is de installatie naam + 'Energiemeter'
        energiemeter_naam = _get_installatienaam_from_kastnaam(kast_naam=kast.naam) + 'Energiemeter'
        typeUuid_energiemeter = 'b4ee4ea9-edd1-4093-bce1-d58918aee281'
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(
                            property='naam',
                            operator=OperatorEnum.EQ,
                            value=energiemeter_naam)]),
                    ExpressionDTO(
                        terms=[TermDTO(
                            property='type',
                            operator=OperatorEnum.EQ,
                            value=typeUuid_energiemeter,
                            logicalOp=LogicalOpEnum.AND)])
                ]))
        energiemeter = next(eminfra_client.search_assets(query_dto=query_dto, actief=True), None)
        if not energiemeter:
            logging.info('Create new asset Energiemeter (OTL)')
            energiemeter_dict = eminfra_client.create_asset_and_relatie(
                assetId=ls.uuid,
                naam=energiemeter_naam,
                typeUuid=typeUuid_energiemeter,
                relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')
            energiemeter = eminfra_client.get_asset_by_id(energiemeter_dict.get("uuid"))
        row["energiemeter_uuid"] = energiemeter.uuid
        row["energiemeter_naam"] = energiemeter.naam
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=energiemeter,
            doelAsset=ls,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["hoortbij_energiemeter_ls"] = relaties[0]


        logging.info('Voedt-relatie toevoegen van DNBLaagspanning naar EnergiemeterDNB')
        relaties = create_relatie_if_missing(
            eminfra_client=eminfra_client,
            bronAsset=dnblaagspanning,
            doelAsset=energiemeter,
            relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')
        if len(relaties) != 1:
            raise ValueError('Multiple relaties exist')
        row["voedt_dnblaagspanning_energiemeterdnb"] = relaties[0]


        logging.info(
            'Elektrische aansluiting overplaatsen van de asset (Legacy) naar EnergiemeterDNB (OTL). '
            'Voorrang geven aan Kast (Legacy) en LSDeel (Legacy). '
            'Pas uitvoeren nadat alle voorgaande checks zijn geïmplementeerd.')
        # todo: Kast vervangen door een meer generieke term, want een elektrische aansluiting kan ook bij andere assets (legacy) voorkomen.
        eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=kast.uuid)

        logging.info('Locatie afleiden via de bestaande relaties')

        rows.append(row)

        logging.debug("Break out the loop after 1 iteration (TEI)")
        break

    logging.info('Write pandas dataframe to an Excel')
    pd.DataFrame(data=rows).to_excel('elektrische aansluitingen voorbereiding Kasten.xlsx', index=False, freeze_panes=[1,1])

    # # kenmerk ophalen
    # # asset_uuid_dummy = '03d3fb2a-aaef-4988-b752-b0298c194063'
    # asset_uuid_dummy = '000536f7-f46b-4cf4-913e-64ec03313083' # TEI
    # elektrisch_aansluitpunt = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)
    #
    # # asset_uuid_dummy = '04a23cea-e851-4d81-b4df-2a9a18b17414'
    # # elektrisch_aansluitpunt = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)
    #
    # # kenmerk wijzigen (json-body leeg maken)
    # # PUT
    # # '/api/assets/{assetId}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b'
    #
    # # Aansluiting loskoppelen
    # eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)
    #
    #
    # logging.info('Filter assets met aansluiting = "A11.FICTIEF". Wis deze eigenschap')
    # df_assets_wissen = df_assets[df_assets['aansluiting'] == 'A11.FICTIEF']
    #
    # # # update de eigenschap met een lege value.
    # # eminfra_client.get_eigenschapwaarden(assetId=, eigenschap_naam='ean-nummer')
    # # eminfra_client.update_eigenschap()
    # #
    # # for _, asset in df_assets.iterrows():
    # #     print("Implement function logic here")