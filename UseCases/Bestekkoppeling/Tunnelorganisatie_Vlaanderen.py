import logging
from datetime import datetime

import pandas as pd
from pathlib import Path
from utils.date_helpers import format_datetime
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment



if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Tunnelorganisatie Vlaanderen: \tBestekkoppelingen beëindigen en nieuwe bestekkoppeling toevoegen')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.TEI
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    eDelta_dossiernummers = ['INTERN-009', 'INTERN-1804', 'INTERN-2050', 'INTERN-2051', 'INTERN-2052', 'INTERN-2055', 'INTERN-2059', 'INTERN-2130', 'INTERN-2131', 'INTERN-2079']
    eDelta_dossiernummers = ['INTERN-009', 'INTERN-1804'] # TODO wissen op productie. Op TEI-omgeving slechts 1 dossiernummer updaten. Dit zijn ~ 44 assets
    logging.info(f'Huidige bestekkoppelingen: {eDelta_dossiernummers}')

    start_datetime = datetime(2025, 1, 1)
    eDelta_dossiernummer_new = 'INTERN-2129' # bestaat ook op TEI
    logging.info(f'Nieuwe bestekkoppeling: {eDelta_dossiernummer_new} heeft startdatum: {start_datetime}')

    output_filepath_excel = Path(__file__).resolve().parent / 'Tunnelorganisatie_Vlaanderen_bestekkoppelingen.xlsx'
    logging.info(f'Output file path: {output_filepath_excel}')

    columns = ["eDelta_dossiernummer", "installatie_uuid", "installatie_naam", "asset_uuid", "asset_naam", "asset_type", "Dossier Nummer", "Bestek Nummer", "Naam aannemer", "Referentie aannemer", "Start koppeling", "Einde koppeling", "Status"]
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
                # expansions=ExpansionsDTO(fields=['parent']),
                selection=SelectionDTO(
                    expressions=[ExpressionDTO(
                        terms=[
                            TermDTO(property='bestek', operator=OperatorEnum.EQ, value=bestekRef_old.uuid)
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

            # search all bestekkoppelingen
            bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

            # get index of the actual bestekkoppeling
            index = next((i for i, item in enumerate(bestekkoppelingen) if item.bestekRef.uuid == bestekRef_old.uuid), None)

            # end bestekkoppeling
            if index is not None and 0 <= index < len(bestekkoppelingen):
                bestekkoppeling_old = bestekkoppelingen[index]
                if bestekkoppeling_old.eindDatum is None:
                    bestekkoppeling_old.eindDatum = format_datetime(start_datetime)

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
                # Insert the new bestekkoppeling at the first index position.
                bestekkoppelingen.insert(index, bestekkoppeling_new)
            # Check if it's active
            else:
                logging.debug(f'Bestekkoppeling "{matching_koppeling.bestekRef.eDeltaDossiernummer}" bestaat al, status: {matching_koppeling.status.value}')
                if matching_koppeling.status.value != 'ACTIEF':
                    logging.debug('Reset start en einddatum om zo deze bestekkoppeling opnieuw te activeren')
                    matching_koppeling.startDatum = format_datetime(start_datetime)
                    matching_koppeling.eindDatum = None


            for item in bestekkoppelingen:
                row_data = {
                    "eDelta_dossiernummer": eDelta_dossiernummer,
                    "installatie_uuid": installatie.uuid,
                    "installatie_naam": installatie.naam,
                    "asset_uuid": asset.uuid,
                    "asset_naam": asset.naam,
                    "asset_type": asset.type.uri,
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
            # todo activeren.
            # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)

    # create dataframe from a list
    df_results = pd.DataFrame(row_list, columns=columns)

    # Write to Excel
    df_results.to_excel(output_filepath_excel, index=False, freeze_panes=[1, 1])