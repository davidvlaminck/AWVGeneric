from prettytable import PrettyTable

from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, construct_naampad
from API.Enums import AuthType, Environment

# requires prettytable, requests, pyjwt

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    asset_types = list(eminfra_client.get_all_otl_assettypes())
    print(f'aantal OTL types: {len(asset_types)}')

    type_term = TermDTO(property='type', operator=OperatorEnum.EQ, value='a7eadedf-b5cf-491b-8b89-ccced9a37004')
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[type_term,
                                     TermDTO(property='actief', operator=OperatorEnum.EQ,
                                             value=True, logicalOp=LogicalOpEnum.AND),
                                     TermDTO(property='beheerobject', operator=OperatorEnum.EQ,
                                             value=None, logicalOp=LogicalOpEnum.AND, negate=True),
                                     TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH,
                                             value='DA-', logicalOp=LogicalOpEnum.AND, negate=True),
                                     TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH,
                                             value='OTN.', logicalOp=LogicalOpEnum.AND, negate=True)])]))
    headers = ['uuid', 'type', 'naampad', 'em_infra_link']
    rows = []
    for otl_asset_type in asset_types:
        print(f'querying type {otl_asset_type.korteUri}')
        type_term.value = otl_asset_type.uuid
        query_dto.from_ = 0
        rows.extend([asset.uuid, otl_asset_type.korteUri, construct_naampad(asset),
                     f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}']
                    for asset in eminfra_client.search_assets(query_dto))
    table = PrettyTable(headers)
    table.add_rows(rows)

    with open('table.csv', 'w', newline='') as f_output:
        f_output.write(table.get_csv_string())
