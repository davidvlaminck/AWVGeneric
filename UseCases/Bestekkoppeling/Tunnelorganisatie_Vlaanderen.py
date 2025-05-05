import logging
from datetime import datetime
from dateutil.parser import isoparse

import pandas as pd
from pathlib import Path

from Generic.ExcelModifier import ExcelModifier
from utils.date_helpers import format_datetime
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling, ApplicationEnum
from API.Enums import AuthType, Environment


def bestekcategorie_is_none(item: BestekKoppeling):
    return item.categorie is None

def set_default_bestekcategorie(item: BestekKoppeling):
    item.categorie = BestekCategorieEnum.WERKBESTEK
    return item


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Tunnelorganisatie Vlaanderen: \tBestekkoppelingen beëindigen en nieuwe bestekkoppeling toevoegen')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.TEI
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    eDelta_dossiernummers = ['INTERN-009', 'INTERN-1804', 'INTERN-2050', 'INTERN-2051', 'INTERN-2052', 'INTERN-2055', 'INTERN-2059', 'INTERN-2130', 'INTERN-2131', 'INTERN-2079']

    logging.info(f'Huidige bestekkoppelingen: {eDelta_dossiernummers}')

    start_datetime = datetime.now()
    eDelta_dossiernummer_new = 'INTERN-2129' # bestaat ook op TEI
    logging.info(f'Nieuwe bestekkoppeling: {eDelta_dossiernummer_new} heeft startdatum: {start_datetime}')

    output_filepath_excel = Path(__file__).resolve().parent / f'Tunnelorganisatie_Vlaanderen_bestekkoppelingen_{environment.name}.xlsx'
    logging.info(f'Output file path: {output_filepath_excel}')

    columns = ["eDelta_dossiernummer", "installatie_naam", "asset_uuid", "asset_naam", "asset_type", "asset_status", "toezichter", "toezichtsgroep", "Bestek volgorde", "Dossier Nummer", "Bestek Nummer", "Naam aannemer", "Referentie aannemer", "Start koppeling", "Einde koppeling", "Status"]
    row_list = []


    # Loop for each eDelta_dossiernummer
    for eDelta_dossiernummer in eDelta_dossiernummers:
        logging.debug(f'Processing eDelta_dossiernummer: {eDelta_dossiernummer}')
        bestekRef_old = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
        bestekRef_new = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer_new)

        query_dto = QueryDTO(
                size=100,
                from_=0,
                pagingMode=PagingModeEnum.OFFSET,
                selection=SelectionDTO(
                    expressions=[ExpressionDTO(
                        terms=[
                            TermDTO(property='actiefBestek', operator=OperatorEnum.EQ, value=bestekRef_old.uuid)
                        ])
                    ])
            )
        logging.debug(f"Assets worden opgehaald via de em-infra API met volgende query: {query_dto}")

        # search all assets:
        assets = eminfra_client.search_assets(query_dto=query_dto)

        # Loop over the assets
        for asset in iter(assets):
            logging.debug(f'Processing asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

            # zoek de top-most-parent-asset, de top van de boomstructuur in het geval van een Legacy-asset.
            installatie = eminfra_client.search_parent_asset(asset_uuid=asset.uuid, recursive=True, return_all_parents=False)
            logging.debug(f'Asset behoort tot de installatie: {installatie.naam}')

            # zoek de toezichter en de toezichtsgroep (Legacy) of de Agent (OTL)
            if 'https://lgc.data.wegenenverkeer.be' in asset.type.uri:
                logging.debug('Asset (Legacy)')
                toezichter_kenmerk = eminfra_client.get_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset.uuid)
                if toezichter_kenmerk.toezichter:
                    toezichter_identiteit = eminfra_client.get_identiteit(toezichter_uuid=toezichter_kenmerk.toezichter.get("uuid"))
                    toezichter_naam = f'{toezichter_identiteit.voornaam} {toezichter_identiteit.naam}'
                else:
                    toezichter_naam = None
                if toezichter_kenmerk.toezichtGroep:
                    toezichtGroep = eminfra_client.get_toezichtgroep(toezichtGroep_uuid=toezichter_kenmerk.toezichtGroep.get("uuid"))
                    toezichtgroep_naam = toezichtGroep.naam
                else:
                    toezichtgroep_naam = None
            elif 'https://wegenenverkeer.data.vlaanderen.be' in asset.type.uri:
                logging.debug('Asset (OTL)')
                logging.debug('OTL-assets vormen een uitzondering, laat toezichter leeg')
                toezichter_naam, toezichtgroep_naam = None, None
            else:
                raise ValueError('Inspect assettype. Assettype is not Legacy or OTL')

            logging.debug(f'De toezichter heet: {toezichter_naam}')
            logging.debug(f'De toezichtgroep heet: {toezichtgroep_naam}')


            # search all bestekkoppelingen
            bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

            # get index of the actual bestekkoppeling
            index = next((i for i, item in enumerate(bestekkoppelingen) if item.bestekRef.uuid == bestekRef_old.uuid), None)

            # end bestekkoppeling
            if index is not None and 0 <= index < len(bestekkoppelingen):
                bestekkoppeling_old = bestekkoppelingen[index]
                if bestekkoppeling_old.eindDatum is None:
                    bestekkoppeling_old.eindDatum = format_datetime(start_datetime)
                    bestekkoppeling_old.status = BestekKoppelingStatusEnum.INACTIEF

            # Check if the new bestekkoppeling doesn't exist and append at the correct index position, else do nothing
            if not (matching_koppeling := next(
                    (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekRef_new.uuid),
                    None, )):
                logging.debug(f'Bestekkoppeling "{bestekRef_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')
                bestekkoppeling_new = BestekKoppeling(
                    bestekRef=bestekRef_new,
                    status=BestekKoppelingStatusEnum.ACTIEF,
                    startDatum=format_datetime(start_datetime),
                    eindDatum=None,
                    categorie=BestekCategorieEnum.WERKBESTEK
                )
                # Insert the new bestekkoppeling at the index position.
                bestekkoppelingen.insert(index, bestekkoppeling_new)
            else:
                logging.debug(f'Bestekkoppeling "{matching_koppeling.bestekRef.eDeltaDossiernummer}" bestaat al, '
                              f'status: {matching_koppeling.status.value}')
                if matching_koppeling.status.value != 'ACTIEF':
                    if isoparse(matching_koppeling.startDatum).replace(tzinfo=None) > start_datetime:
                        logging.debug(
                            'De huidige startdatum van het bestek ligt in de toekomst. Reset de startdatum naar vandaag'
                        )
                        matching_koppeling.startDatum = format_datetime(start_datetime)
                    if matching_koppeling.eindDatum is not None:
                        logging.debug(f'Wis de eindDatum: {matching_koppeling.eindDatum}. Set None')
                        matching_koppeling.eindDatum = None
                    logging.debug('Activeer de bestekkoppeling manueel')
                    matching_koppeling.status = BestekKoppelingStatusEnum.ACTIEF

            # Herorden de volgorde van de bestekkoppelingen: alle inactieve onderaan de lijst.
            bestekkoppelingen = sorted(bestekkoppelingen, key=lambda x: x.status.value, reverse=False)

            # Voeg default bestekcategorie toe indien die ontbreekt.
            for item in bestekkoppelingen:
                if bestekcategorie_is_none(item):
                    set_default_bestekcategorie(item)

            for index, item in enumerate(bestekkoppelingen):
                row_data = {
                    "eDelta_dossiernummer": eDelta_dossiernummer,
                    "installatie_naam": installatie.naam,
                    "asset_uuid": asset.uuid,
                    "asset_naam": asset.naam,
                    "asset_type": asset.type.uri,
                    "asset_status": 'ACTIEF' if asset.actief else 'INACTIEF',
                    "toezichter": toezichter_naam,
                    "toezichtsgroep": toezichtgroep_naam,
                    "Bestek volgorde": index+1 if item.status.value == 'ACTIEF' else None,  # 1-based index voor de actieve bestekkoppelingen
                    "Dossier Nummer": item.bestekRef.eDeltaDossiernummer,
                    "Bestek Nummer": item.bestekRef.eDeltaBesteknummer,
                    "Naam aannemer": item.bestekRef.aannemerNaam,
                    "Referentie aannemer": item.bestekRef.aannemerReferentie,
                    "Start koppeling": item.startDatum,
                    "Einde koppeling": item.eindDatum,
                    "Status": item.status.value
                }
                row_list.append(row_data)

            # Update all the bestekkoppelingen for this asset
            # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)


    # create dataframe from a list
    df_results = pd.DataFrame(row_list, columns=columns)

    # Write to Excel
    df_results.to_excel(output_filepath_excel, index=False, freeze_panes=[1, 1])

    # Installeer een link naar em-infra
    ExcelModifier(file_path=output_filepath_excel).add_hyperlink(sheet='Sheet1', link_type=ApplicationEnum.EM_INFRA, env=environment)