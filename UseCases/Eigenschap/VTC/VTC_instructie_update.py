import logging
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import KenmerkTypeEnum, EigenschapValueUpdateDTO, EigenschapValueDTO

from UseCases.utils import load_settings_path, configure_logger, vervang_door_verweven_asset

INPUT_EXCEL = (Path.home() / 'OneDrive - Vlaamse overheid - Office 365/0_projecten_awv/Tunnels/VTC-instructie'
               / 'VTC_instructies_Tunnel_Patrick.xlsx')

if __name__ == '__main__':
    configure_logger()
    logging.info('Update eigenschap Instructie VTC.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings_path())

    usecols = ['tunnel', 'uuid', 'naampad', 'VTC-instructie']
    df_assets = pd.read_excel(io=INPUT_EXCEL, sheet_name='Sheet1', usecols=usecols)

    vtc_eigenschap = eminfra_client.eigenschap_service.list_eigenschap(kenmerktype_id='f38c7a7f-39e9-442e-9ec8-8393e5784f0c')
    rows = []
    for idx, df_row in df_assets.iterrows():
        asset_uuid = df_row["uuid"]
        logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        asset = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=asset_uuid)

        asset = vervang_door_verweven_asset(client=eminfra_client, asset=asset)

        # Bestaat er een kenmerk: Instructie VTC?
        kenmerktype_vtc = eminfra_client.kenmerk_service.get_kenmerken_by_uuid(asset_uuid=asset.uuid, naam=KenmerkTypeEnum.VTC)
        if len(kenmerktype_vtc) == 1:
            logging.info(f'Asset {asset.uuid} bevat het kenmerk VTC-instructie.')

            # Is het kenmerk ingevuld? Met andere woorden: bevat de eigenschap een waarde?
            eigenschapwaarde_vtc_instructies = eminfra_client.eigenschap_service.get_eigenschapwaarden(
                asset_uuid=asset.uuid,
                kenmerk_uuid=kenmerktype_vtc[0].type["uuid"])
            if len(eigenschapwaarde_vtc_instructies) == 0:
                logging.debug('Eigenschapwaarde ontbreekt: vul aan.')
                eigenschapwaarde_update_vtc_instructie = EigenschapValueUpdateDTO(
                    typedValue={'_type': 'text', 'value': df_row["VTC-instructie"]},
                    eigenschap=vtc_eigenschap[0]
                )
                # Update de (lege) eigenschap "instructie voor het VTC".
                eminfra_client.eigenschap_service.update_eigenschap_by_uuid(asset_uuid=asset.uuid,
                                                                            eigenschap=eigenschapwaarde_update_vtc_instructie,
                                                                            kenmerktype=kenmerktype_vtc[0])
            elif len(eigenschapwaarde_vtc_instructies) == 1:
                logging.info('Eigenschapwaarde bestaat: doe niets / skip asset.')
                eigenschapwaarde_vtc_instructie = eigenschapwaarde_vtc_instructies[0]
                logging.debug(f'Huidige waarde: {eigenschapwaarde_vtc_instructie.typedValue['value']}')
            else:
                raise ValueError('Meerdere lijst elementen in variabele eigenschapwaarde_vtc_instructies')
        else:
            logging.info(f'Asset {asset.uuid} bevat NIET het kenmerk VTC-instructie.')