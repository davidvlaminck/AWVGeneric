import logging
from datetime import datetime
from openpyxl import load_workbook

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Omkeren van em-infra wijzigingen:\t Terugdraaien van de wijzigingen uitgevoerd door Patrick Van Ransbeek op 09/10/20025 tussen 11:45 en 11:50.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Ophalen van alle wijzigingen.")
    identiteit_patrick = next(eminfra_client.search_identiteit(naam='Patrick Van Ransbeek'), None)
    created_after = datetime(year=2025, month=10, day=9, hour=11, minute=45, second=0)
    created_before = datetime(year=2025, month=10, day=9, hour=11, minute=50, second=0)
    events = eminfra_client.search_events(created_by=identiteit_patrick, created_after=created_after, created_before=created_before)

    logging.info("Prioritiseren en sorteren van de wijzigingen op basis van Event.")
    logging.info("API geeft alle events terug van een specifieke dag. De resultaten moeten verder gefilterd worden op basis van de tijd.")
    events_list = list(events)
    events_list = [e for e in events_list if created_after <= datetime.fromisoformat(e.createdOn).replace(tzinfo=None) <= created_before]
    event_types = {e.type.name for e in events_list}
    logging.info(f"Aantal wijzigingen: {len(events_list)}")
    logging.info(f"Aantal event-types: {len(event_types)}\n\t{event_types}")
    logging.info("Aanpassingen terugdraaien.")

    rows = []

    for idx, event in enumerate(events_list):
        logging.debug(f'Processing event\n\t# {idx}\n\tuuid: {event.data.get("aggregateId").get("uuid")}\n\tevent type: {event.data.get("_type")}')
        event_type = event.type.name
        asset_uuid = event.data.get("aggregateId").get("uuid")
        asset_type = event.data.get("aggregateId").get("_type")
        asset = eminfra_client.get_asset_by_id(assettype_id=asset_uuid)

        row = {
            "uuid": asset_uuid
            , "link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset_uuid}'
            , "asset_type": asset_type
            , "event_type": event_type
            , "aanpassingsdatum": event.createdOn
            , "from": event.data.get("from")
            , "to": event.data.get("to")
        }

        # event types are implemented in alphabetical order.
        match event_type:
            case 'ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED':
                eigenschap_kenmerk = event.data.get('from')
                kenmerktype_uuid = event.data.get("kenmerkTypeId").get("uuid")
                if eigenschap_kenmerk.get('values') == {}:
                    logging.debug('Eigenschap leegmaken')
                    # eminfra_client.update_kenmerk(asset_uuid=asset_uuid, kenmerk_uuid=kenmerktype_uuid, request_body='{}')
                else:
                    logging.debug(f"Vorige eigenschap (of eigenschappen?) terugplaatsen: {eigenschap_kenmerk}")
                    request_body = event.data.get('to').get('values').values()
                    # eminfra_client.update_kenmerk(asset_uuid=asset_uuid, kenmerk_uuid=kenmerktype_uuid, request_body=request_body)
            case 'ASSET_PARENT_UPDATED':
                logging.critical('Navragen aan Patrick dat deze wijziging mag wordne geïmplementeerd.')
                parent_asset = event.data.get("from")
                if parent_asset:
                    logging.debug(f"Asset '{asset_uuid}' in boomstructuur plaatsen onder: {parent_asset}. Controleer het type: beheerobject of installatie.")
            case 'ASSET_TOEZICHTER_UPDATED':
                toezichter = event.data.get("from")
                logging.debug(f'Toezichter terugplaatsen: {toezichter}.')
                # eminfra_client.add_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset_uuid, toezichter_uuid=toezichter.get("uuid"))
            case 'ASSET_TOEZICHT_GROEP_UPDATED':
                toezichtsgroep = event.data.get("from")
                logging.debug(f'Toezichtsgroep terugplaatsen: {toezichtsgroep}.')
                # eminfra_client.add_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset_uuid, toezichtgroep_uuid=toezichtsgroep.get("uuid"))
            case 'INSTALLATIE_ACTIEF_UPDATED':
                installatie_status = event.data.get("from")
                logging.debug(f"Installatie status terugplaatsen: {installatie_status}")
                # if installatie_status == 'ACTIEF':
                #     eminfra_client.activeer_asset(asset=asset)
                # elif installatie_status == 'INACTIEF':
                #     eminfra_client.deactiveer_asset(asset=asset)
                # else:
                #     raise NotImplementedError(installatie_status)
            case 'INSTALLATIE_COMMENTAAR_UPDATED':
                installatie_commentaar = event.data.get("from")
                logging.debug(f"Installatie commentaar terugplaatsen: {installatie_commentaar}")
                # eminfra_client.update_asset(uuid=asset_uuid, commentaar=installatie_commentaar)
            case 'INSTALLATIE_NAAM_UPDATED':
                installatie_naam = event.data.get("from")
                logging.debug(f'Installatie naam terugplaatsen: {installatie_naam}.')
                # eminfra_client.update_asset(uuid=asset_uuid, naam=installatie_naam)
            case 'INSTALLATIE_TOESTAND_UPDATED':
                installatie_toestand = event.data.get("from")
                logging.debug(f"Installatie toestand terugplaatsen: {installatie_toestand}")
                # eminfra_client.update_asset(uuid=asset_uuid, toestand=installatie_toestand)

        rows.append(row)

    df = pd.DataFrame(rows)

    # Define Excel file path
    excel_path = "terugplaatsen_em-infra.xlsx"

    # 1️⃣ Export DataFrame to Excel using openpyxl engine
    df.to_excel(excel_path, index=False, sheet_name="historiek", engine="openpyxl")

    # 2️⃣ Reopen with openpyxl to modify settings
    workbook = load_workbook(excel_path)
    sheet = workbook.active  # or workbook["People"]

    # 3️⃣ Freeze the first row and first column
    sheet.freeze_panes = sheet["B2"]  # freezes row 1 and column A

    # 4️⃣ Save changes
    workbook.save(excel_path)