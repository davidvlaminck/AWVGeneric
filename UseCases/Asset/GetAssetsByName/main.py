from pathlib import Path

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import TermDTO, QueryDTO, OperatorEnum, PagingModeEnum, ExpansionsDTO, SelectionDTO, \
    ExpressionDTO, LogicalOpEnum
from API.Enums import Environment, AuthType


if __name__ == '__main__':
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    type_term = TermDTO(property='type', operator=OperatorEnum.EQ, value='c505b262-fe1f-42cb-970f-7f44487b24ec')

    kabelnettoegang_naam = 'dummyNaam'
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[type_term,
                                        TermDTO(property='actief', operator=OperatorEnum.EQ,
                                                value=True, logicalOp=LogicalOpEnum.AND)
                                     ,TermDTO(property='naam', operator=OperatorEnum.EQ,
                                              value=f"{kabelnettoegang_naam}", logicalOp=LogicalOpEnum.AND)
                                        ]
                             )]))

    generator_assets = eminfra_client.assets.search_assets(query_dto=query_dto)
    generator_assets_list = list(generator_assets)
    print(f'Length of the list: {len(generator_assets_list)}')