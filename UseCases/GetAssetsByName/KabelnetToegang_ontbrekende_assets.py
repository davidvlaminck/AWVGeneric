import uuid
import re

from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_model.OtlmowModel.Classes.Onderdeel.HoortBij import HoortBij

import json
import os

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import TermDTO, QueryDTO, OperatorEnum, PagingModeEnum, ExpansionsDTO, SelectionDTO, \
    ExpressionDTO, LogicalOpEnum
from API.Enums import Environment, AuthType


if __name__ == '__main__':
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    type_term = TermDTO(property='type', operator=OperatorEnum.EQ, value='c505b262-fe1f-42cb-970f-7f44487b24ec')

    excel_file = Path().home() / 'Downloads' / "filename.xlsx"
    df_assets = pd.read_excel(excel_file, sheet_name="onderdeel#HoortBij", index_col=None, usecols=["typeURI", "assetId.identificator", "bronAssetId.identificator", "bron_naam", "doelAssetId.identificator"])

    # loop over the assets
    created_assets = []
    # for index in range(0, 1, 1):
    for index in range(len(df_assets)):
        kabelnettoegang_naam = df_assets.iloc[index].bron_naam
        # Apply regex replace
        kabelnettoegang_naam = re.sub("[/\[\]\s\.:]", "_", kabelnettoegang_naam)

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

        generator_assets = eminfra_client.search_assets(query_dto=query_dto)
        generator_assets_list = list(generator_assets)
        print(f'Length of the list: {len(generator_assets_list)}')

        if generator_assets_list:
            asset_uuid = generator_assets_list[0].uuid
        else:
            asset_uuid = None

        # Create otlmow-object and fill with data.
        # Leg de HoortBij-relatie van de KabelnetToegang (bron) (OTL) naar de Teletransmissieverbinding (doel) (LGC)
        asset_HoortBij = HoortBij()
        asset_HoortBij.assetId.identificator = df_assets.iloc[index].get("assetId.identificator")
        asset_HoortBij.bronAssetId.identificator = f'{asset_uuid}-b25kZXJkZWVsI0thYmVsbmV0VG9lZ2FuZw'
        asset_HoortBij.bron.typeURI = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#KabelnetToegang'
        asset_HoortBij.doelAssetId.identificator = df_assets.iloc[index].get("doelAssetId.identificator")
        asset_HoortBij.doel.typeURI = 'https://lgc.data.wegenenverkeer.be/ns/installatie#TT'
        asset_HoortBij.isActief = True

        created_assets.append(asset_HoortBij)

    # OTLMOW-converter to write to a DAVIE-file (OTLMOW-Converter)
    OtlmowConverter.from_objects_to_file(file_path=Path('HoortBijRelaties_DA-2025-XXXXX.xlsx'), sequence_of_objects=created_assets)