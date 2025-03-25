import os
import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, ExpressionDTO, OperatorEnum, \
    TermDTO, AssetDTO
from API.Enums import AuthType, Environment

from pathlib import Path


def return_beheerobject(asset: AssetDTO, depth=0):
    # beheerobject
    if getattr(asset, "parent", None) is None:
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
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    filepath = Path().home() / 'Downloads' / 'MigratieLS' / 'RSA Laagspanningsaansluiting (Legacy) keuringsinfo.xlsx'
    df_ls_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2,
                                 usecols=["uuid", "naampad", "bevat_keuringsinfo"])

    df_ls_assets.query(expr='`bevat_keuringsinfo` == True & naampad.notna() & naampad != "TYS.TUNNEL/A2799/Kast/LS"', inplace=True)

    for index, df_ls_asset in df_ls_assets.iterrows():
        # print(f'Index: {index} - asset: {df_ls_asset}')
        # ophalen van de asset ls
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(property='id', operator=OperatorEnum.EQ, value=f'{df_ls_asset.uuid}')]
                    )]))

        ls_asset = next(eminfra_client.search_assets(query_dto=query_dto))
        print(f'Processing: {ls_asset.uuid}')
        # ophalen van de parent-asset in de boomstructuur. Meestal is dit een Kast
        parent_asset = next(eminfra_client.search_parent_asset(asset_uuid=ls_asset.uuid), None)
        if not parent_asset:
            raise ValueError(f'Parent is missing for asset: {ls_asset.uuid}')

        # ophalen van de installatie naam
        # Dit is de naam van de dieperliggende, geneste, parent-asset
        beheerobject, diepte = return_beheerobject(parent_asset)
        try:
            installatie_naam = beheerobject.naam
        except AttributeError:
            installatie_naam = beheerobject.get("naam")

        # ophalen van alle child-assets van de parent-asset
        child_assets = list(eminfra_client.search_child_assets(asset_uuid=parent_asset.uuid))

        # als child-asset LSDeel ontbreekt > aanmaken LSDeel in de boomstructuur
        lsdeel_type_uuid = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
        lst_lsdeel_asset = list(child_asset for child_asset in child_assets if child_asset.type.uuid == lsdeel_type_uuid)

        if len(lst_lsdeel_asset) > 2:
            raise ValueError(f'Multiple assets LSDeel present in tree of: {parent_asset.uuid}.')
        elif len(lst_lsdeel_asset) == 1:
            lsdeel_asset = lst_lsdeel_asset[0]
        else:
            print(f'No asset LSDeel present in tree of: {parent_asset.uuid}.\nCreating instance...')
            # aanmaken LSDeel in de boomstructuur
            eminfra_client.create_lgc_asset(parent_uuid=parent_asset.uuid, naam=f'{installatie_naam}.LSDeel',
                                            typeUuid=lsdeel_type_uuid)
            # herinstantiëren van child_assets
            child_assets = list(eminfra_client.search_child_assets(asset_uuid=parent_asset.uuid))

            # herinstantiëren van lsdeel_asset
            lsdeel_asset = next((child_asset for child_asset in child_assets if
                                 child_asset.type.uuid == "b4361a72-e1d5-41c5-bfcc-d48f459f4048"), None)


        # ophalen van de Voedingsrelatie van de LS
        kenmerkTypeId_voedt = '91d6223c-c5d7-4917-9093-f9dc8c68dd3e'  # Voedt
        relatieTypeId_voedt = 'f2c5c4a1-0899-4053-b3b3-2d662c717b44'
        voedingsrelaties_ls = list(
            eminfra_client.search_relaties(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_voedt,
                                           relatieTypeId=relatieTypeId_voedt))

        # als Voedingsrelatie van LS naar LSDeel ontbreekt > aanmaken Voedingsrelatie
        if not any(relatie.uuid == lsdeel_asset.uuid for relatie in voedingsrelaties_ls):
            try:
                eminfra_client.add_relatie(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_voedt,
                                       relatieTypeId=relatieTypeId_voedt, doel_assetId=lsdeel_asset.uuid)
            except Exception as e:
                print(f'Voedings relatie kan niet gelegd worden van asset: {ls_asset.uuid} naar: {lsdeel_asset.uuid}')


        # ophalen bevestigingsrelatie van de LS
        kenmerkTypeId_bevestiging = 'c3494ff0-9e02-4c11-856c-da8db6238768'  # Bevestiging (bevestigd aan)
        relatieTypeId_bevestiging = '3ff9bf1c-d852-442e-a044-6200fe064b20'
        bevestigingsrelaties_ls = list(
            eminfra_client.search_relaties(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging,
                                           relatieTypeId=relatieTypeId_bevestiging))

        # als bevestigingsrelatie onbreekt van LS naar parent-asset > aanmaken
        if not any(relatie.uuid == parent_asset.uuid for relatie in bevestigingsrelaties_ls):
            try:
                eminfra_client.add_relatie(assetId=ls_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging,
                                       relatieTypeId=relatieTypeId_bevestiging, doel_assetId=parent_asset.uuid)
            except Exception as e:
                print(f'Bevestiging relatie kan niet gelegd worden tussen asset: {ls_asset.uuid} en diens parent: {parent_asset.uuid}')

        # ophalen bevestigingsrelatie van de LSDeel
        bevestigingsrelaties_lsdeel = list(
            eminfra_client.search_relaties(assetId=lsdeel_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging,
                                           relatieTypeId=relatieTypeId_bevestiging))

        # als bevestigingsrelatie onbreekt van LSDeel naar parent-asset > aanmaken
        if not any(relatie.uuid == parent_asset.uuid for relatie in bevestigingsrelaties_lsdeel):
            try:
                eminfra_client.add_relatie(assetId=lsdeel_asset.uuid, kenmerkTypeId=kenmerkTypeId_bevestiging,
                                       relatieTypeId=relatieTypeId_bevestiging, doel_assetId=parent_asset.uuid)
            except Exception as e:
                print(f'Bevestiging relatie kan niet gelegd worden tussen asset: {lsdeel_asset.uuid} en diens parent: {parent_asset.uuid}')

        # ophalen alle beschikbare eigenschappen LS
        eigenschappen_ls = eminfra_client.get_eigenschappen(assetId=ls_asset.uuid)
        eigenschappen_lsdeel = eminfra_client.get_eigenschappen(assetId=lsdeel_asset.uuid)

        # kopiëren van de eigenschappen van LS naar LSDeel: "datum eerste controle", "datum laatste keuring", "resultaat keuring"
        # filter eigenschappen LS op basis van 3 eigenschappen die we willen overplaatsen.
        eigenschappen_ls_keuring = []
        for item in eigenschappen_ls:
            if item.eigenschap.uri in ('https://ins.data.wegenenverkeer.be/ns/attribuut#EMObject.resultaatKeuring',
                                       'https://ins.data.wegenenverkeer.be/ns/attribuut#EMObject.datumLaatsteKeuring',
                                       'https://ins.data.wegenenverkeer.be/ns/attribuut#EMObject.datumEersteControle'
                                       ):
                eigenschappen_ls_keuring.append(item)

        # controleer welke van deze drie eigenschappen nog niet bestaan bij LSDeel
        new_eigenschappen = []
        new_eigenschappen.extend(item for item in eigenschappen_ls_keuring if item not in eigenschappen_lsdeel)

        kenmerken = eminfra_client.get_kenmerken(assetId=lsdeel_asset.uuid)
        kenmerk_uuid = next(kenmerk.type.get('uuid') for kenmerk in kenmerken if kenmerk.type.get('naam').startswith('Eigenschappen'))
        # Loop over het resultaat en genereer de eigenschappen voor het LSDeel
        for new_eigenschap in new_eigenschappen:
            eminfra_client.update_eigenschap(asset_uuid=lsdeel_asset.uuid,
                                             kenmerk_uuid=kenmerk_uuid,
                                             eigenschap=new_eigenschap.eigenschap,
                                             typedValue=new_eigenschap.typedValue)