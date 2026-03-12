from prettytable import PrettyTable

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, construct_naampad
from API.Enums import AuthType, Environment
from UseCases.utils import load_settings_path

ENVIRONMENT = Environment.PRD

if __name__ == '__main__':
    settings_path = load_settings_path()
    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=settings_path)

    asset_types = list(eminfra_client.assettype_service.get_all_assettypes())
    print(f'aantal OTL types: {len(asset_types)}')

    type_term = TermDTO(property='type', operator=OperatorEnum.EQ, value='a7eadedf-b5cf-491b-8b89-ccced9a37004')
    headers = ['uuid', 'type', 'naampad', 'em_infra_link']
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     # type_term
                                     # ,
                                     TermDTO(property='"actiefOfToekomstigBestek"', operator=OperatorEnum.EQ,
                                             value='09125be9-febd-471d-bec4-ae57ef3e5800')
                                     , TermDTO(property='actief', operator=OperatorEnum.EQ,
                                             value=True, logicalOp=LogicalOpEnum.AND)
                                     # , TermDTO(property='beheerobject', operator=OperatorEnum.EQ,
                                     #         value=None, logicalOp=LogicalOpEnum.AND, negate=True)
                                     # , TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH,
                                     #         value='DA-', logicalOp=LogicalOpEnum.AND, negate=True)
                                     # , TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH,
                                     #         value='OTN.', logicalOp=LogicalOpEnum.AND, negate=True)
                                 ])
                             ])
                         )
    rows = []
    for otl_asset_type in asset_types:
        print(f'querying type {otl_asset_type.korteUri}')
        type_term.value = otl_asset_type.uuid
        query_dto.from_ = 0
        rows.extend([asset.uuid, otl_asset_type.korteUri, construct_naampad(asset),
                     f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}']
                    for asset in eminfra_client.asset_service.search_assets_generator(query_dto))
    table = PrettyTable(headers)
    table.add_rows(rows)

    with open('table.csv', 'w', newline='') as f_output:
        f_output.write(table.get_csv_string())
