from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    # Step 1. Search assets "Geluidwerende constructie", including the parent asset.
    query_dto = QueryDTO(
        size=5,
        from_=0,
        pagingMode=PagingModeEnum.OFFSET,
        expansions={"fields": ["parent"]},
        selection=SelectionDTO(
            expressions=[
                # first element: assettype = installatie#GeluidwerendeConstructie
                ExpressionDTO(
                    terms=[
                        TermDTO(
                            property='type',
                            operator=OperatorEnum.EQ,
                            value='60698ead-a2d8-4698-9638-83e1163fb9fd')]
                )
                # second element: actief = 'true'
                # ,
                # ExpressionDTO(
                #     logicalOp=LogicalOpEnum.AND,
                #     terms=[
                #         TermDTO(
                #             property='actief',
                #             operator=OperatorEnum.EQ,
                #             value='true')]
                # )
            ]
        )
    )

    # Step 2. Store all the assets in a list.
    assets_with_parent = [
        asset
        for asset in eminfra_client.search_assets(query_dto=query_dto)
        if asset.parent
    ]

    # Step 3. Launch API call to remove the asset from the tree
    for asset in assets_with_parent:
        eminfra_client.remove_parent_from_asset(parent_uuid=asset.parent['uuid'], asset_uuid=asset.uuid)