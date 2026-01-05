import json


from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, BestekKoppeling, construct_naampad
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documenten/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.AIM, auth_type=AuthType.JWT, settings_path=settings_path)

    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(                                 terms=[
                                        TermDTO(property='actief', operator=OperatorEnum.EQ,
                                                value=True)
                                     , TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH,
                                               value='MAR.TUNNEL', logicalOp=LogicalOpEnum.AND)
                                        ]
                             )]))

    generator_assets = eminfra_client.search_assets(query_dto=query_dto)

    def to_serializable(bk: BestekKoppeling):
        return json.loads(bk.json())

    solution_list = []
    for index, asset in enumerate(generator_assets):
        print(index)
        asset_d = {'asset_uuid': asset.uuid, 'naampad': construct_naampad(asset)}
        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)
        asset_d['bestekkoppelingen'] = [to_serializable(bk) for bk in bestekkoppelingen]
        solution_list.append(asset_d)

    # create a new file or overwrite
    with open('mar.tunnel_koppelingen.json', 'w') as json_file:
        json.dump(solution_list, json_file, indent=2)