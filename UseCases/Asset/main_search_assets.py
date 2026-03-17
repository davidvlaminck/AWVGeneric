import logging

from utils.wkt_geometry_helpers import format_locatie_kenmerk_lgc_2_wkt

from prettytable import PrettyTable

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, construct_naampad
from API.Enums import AuthType, Environment
from UseCases.utils import load_settings_path, configure_logger

ENVIRONMENT = Environment.PRD
# BESTEK_UUID = '09125be9-febd-471d-bec4-ae57ef3e5800' # INTERN-099
BESTEK_UUID = 'b59d4323-039f-4472-a0b9-e025a88fa79d'  # AWV/VW/2025/3

def get_wkt(asset):
    try:
        locatieKenmerk = eminfra_client.locatie_service.get_locatie_by_uuid(asset_uuid=asset.uuid)
        return format_locatie_kenmerk_lgc_2_wkt(locatie=locatieKenmerk)
    except Exception as e:
        return None

if __name__ == '__main__':
    configure_logger()
    logging.info(f'Generic script to search assets')

    settings_path = load_settings_path()
    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=settings_path)

    expression_actief = ExpressionDTO(
        terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)]
        , logicalOp=None
    )
    expression_bestek = ExpressionDTO(
        terms=[TermDTO(property='actiefOfToekomstigBestek', operator=OperatorEnum.EQ,
                value=BESTEK_UUID)]  # INTERN-099
        , logicalOp=LogicalOpEnum.AND
    )

    query_dto = QueryDTO(
        size=100,
        from_=0,
        pagingMode=PagingModeEnum.OFFSET,
        expansions=ExpansionsDTO(fields=['parent']),
        selection=SelectionDTO(
            expressions=[
                expression_actief,
                expression_bestek])
    )

    headers = ["naam", "typeURI", "assetId.identificator", "geometry", "naampad", "link"]
    rows = []
    rows.extend(
        [
            asset.naam,
            asset.uuid,
            asset.type.uri,
            get_wkt(asset),
            construct_naampad(asset),
            f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}']
        for asset in eminfra_client.asset_service.search_assets_generator(query_dto)
    )
    table = PrettyTable(headers)
    table.add_rows(rows)

    with open('table_bestekkoppelingen_AWV_VW_2025_3.csv', 'w', newline='') as f_output:
        f_output.write(table.get_csv_string())

    # instances = []
    # for asset in eminfra_client.asset_service.search_assets_generator(query_dto):
    #     instance = dynamic_create_instance_from_uri(asset.type.uri)
    #     instance.assetId.identificator = asset.uuid
    #     instance.assetId.toegekendDoor = 'AWV'
    #     try:
    #         locatieKenmerk = eminfra_client.locatie_service.get_locatie_by_uuid(asset_uuid=asset.uuid)
    #         wkt_string = format_locatie_kenmerk_lgc_2_wkt(locatie=locatieKenmerk)
    #         instance.geometry = wkt_string
    #     except Exception as e:
    #         instance.geometry = None
    #     instance.isActief = asset.actief
    #     instance.naam = asset.naam
    #     instance.naampad = construct_naampad(asset)
    #     instance.notitie = asset.commentaar
    #     instance.toestand = asset.toestand.value.lower().replace('_', '-')
    #     logging.info("Append instance")
    #     instances.append(instance)
    #
    # OtlmowConverter.from_objects_to_file(file_path=Path('new_assets.xlsx'), sequence_of_objects=instances)
