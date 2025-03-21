import os
import random
from datetime import datetime

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, ExpressionDTO, OperatorEnum, \
    TermDTO, LogicalOpEnum, AssetDTO
from API.Enums import AuthType, Environment

from pathlib import Path


def return_beheerobject(asset: AssetDTO, depth=0):
    # beheerobject
    if asset.parent is None:
        return asset, depth

    # another object
    current_asset = asset.parent
    current_depth = depth + 1

    # recursive function call
    parent_asset, parent_depth = return_beheerobject(current_asset, current_depth)

    if parent_asset and parent_depth > current_depth:
        return parent_asset, parent_depth
    else:
        return current_asset, current_depth

if __name__ == '__main__':
    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    filepath = Path().home() / 'Downloads' / 'MigratieLS' / 'RSA Laagspanningsaansluiting (Legacy) keuringsinfo.xlsx'
    df_ls_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2,
                                 usecols=["uuid", "naampad", "bevat_keuringsinfo"])

    df_ls_assets.query(expr='`bevat_keuringsinfo` == True', inplace=True)

    for index, df_ls_asset in df_ls_assets.iloc[:1].iterrows():  # todo remove the slicing of the dataframe
        print(f'Index: {index} - asset: {df_ls_asset}')
        # ophalen van de asset ls
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(property='id', operator=OperatorEnum.EQ, value=f'{df_ls_asset.uuid}')]
                    )]))

        ls_asset = next(eminfra_client.search_assets(query_dto=query_dto))

        # ophalen van de parent-asset in de boomstructuur. Meestal is dit een Kast
        parent_asset = next(eminfra_client.search_parent_asset(asset_uuid=ls_asset.uuid))
        if not parent_asset:
            raise ValueError(f'Parent is missing for asset: {ls_asset.uuid}')

        # ophalen van de installatie naam
        # Dit is de naam van de dieperliggende, geneste, parent-asset
        beheerobject, diepte = return_beheerobject(parent_asset)
        installatie_naam = beheerobject.naam

        # ophalen van alle child-assets van de parent-asset
        child_assets = list(eminfra_client.search_child_assets(asset_uuid=parent_asset.uuid))

        # als child-asset LSDeel ontbreekt > aanmaken LSDeel in de boomstructuur
        lsdeel_asset = next((child_asset for child_asset in child_assets if
                             child_asset.type.uuid == "b4361a72-e1d5-41c5-bfcc-d48f459f4048"), None)

        if not lsdeel_asset:
            # aanmaken LSDeel in de boomstructuur
            random_integer = random.randrange(1000000)
            eminfra_client.create_lgc_asset(parent_uuid=parent_asset.uuid, naam=f'{installatie_naam}.LSDeel.{random_integer}',
                                            typeUuid='b4361a72-e1d5-41c5-bfcc-d48f459f4048')
        else:
            print(f'Parent asset {parent_asset.uuid} heeft een child-asset LSDeel: {lsdeel_asset.uuid}')

        # todo ophalen van de uuid van alle relatietypes en selecteer nadien de uuid van de Voedingsrelatie

        # ophalen van de Voedingsrelatie van de LS
        kenmerkTypeId_voedt = '91d6223c-c5d7-4917-9093-f9dc8c68dd3e'  # Voedt
        relatieTypeId_voedt = 'f2c5c4a1-0899-4053-b3b3-2d662c717b44'
        voedingsrelaties_ls = list(
            eminfra_client.search_relaties(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_voedt, relatieTypeId=relatieTypeId_voedt))

        # als Voedingsrelatie van LS naar LSDeel ontbreekt > aanmaken Voedingsrelatie
        if not voedingsrelaties_ls:
            eminfra_client.add_relatie(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_voedt, relatieTypeId=relatieTypeId_voedt, doel_assetId=lsdeel_asset.uuid)

        # ophalen bevestigingsrelatie van de LS
        kenmerkTypeId_bevestiging = 'c3494ff0-9e02-4c11-856c-da8db6238768'  # Bevestiging
        relatieTypeId_bevestiging = '3ff9bf1c-d852-442e-a044-6200fe064b20'
        bevestigingsrelaties_ls = list(
            eminfra_client.search_relaties(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging, relatieTypeId=kenmerkTypeId_bevestiging))

        # als bevestigingsrelatie onbreekt van LS naar parent-asset > aanmaken
        # TODO: check aanpassen: bevestigingsrelatie VAN ls_asset NAAR parent_asset
        if not bevestigingsrelaties_ls:
            eminfra_client.add_relatie(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging, relatieTypeId=kenmerkTypeId_bevestiging, doel_assetId=parent_asset.uuid)

        # ophalen bevestigingsrelatie van de LSDeel
        bevestigingsrelaties_lsdeel = list(
            eminfra_client.search_relaties(assetId=lsdeel_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging, relatieTypeId=kenmerkTypeId_bevestiging))

        # als bevestigingsrelatie onbreekt van LSDeel naar parent-asset > aanmaken
        # TODO: check aanpassen: bevestigingsrelatie VAN lsdeel_asset NAAR parent_asset
        if not bevestigingsrelaties_lsdeel:
            eminfra_client.add_relatie(assetId=lsdeel_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging, relatieTypeId=relatieTypeId_bevestiging, doel_assetId=parent_asset.uuid)

# ophalen alle beschikbare eigenschappen LS
# kopiëren van alle beschikbare eigenschappen van LS naar LSDeel


