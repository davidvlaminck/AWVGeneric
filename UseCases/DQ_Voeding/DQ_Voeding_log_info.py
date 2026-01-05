import logging
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import RelatieEnum, AssetDTO
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger
from utils.toezichter_helpers import get_toezichter_naam

INSTALLATIE_TYPES = {
    "LS": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
    "LSDEEL": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel",
    "HS": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS",
    "HSDEEL": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel",
    "KAST": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
}
ONDERDEEL_TYPES = {
    "DNB_LAAG": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning",
    "DNB_HOOG": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning",
    "EM_DNB": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB",
    "FORFAIT": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting",
}


def extract_info(asset_list: [AssetDTO]) -> str:
    """
    Extract info from a list of AssetDTO.
    Parse assettypes and names
    """
    return '; '.join([f'assettype: {asset.type.korteUri}, naam: {asset.naam}' for asset in asset_list])


def filter_assets(assets: list[AssetDTO], type_uri: str) -> list[AssetDTO]:
    """
    Filter from a list of assets the active assets that match an uri.
    """
    return [a for a in assets if a.type.uri == type_uri and a.actief]


if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.')
    environment = Environment.PRD
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=load_settings())

    input_file = Path.home() / 'Nordend/AWV - Documents/DataQuality/Voeding' / f'DQ Voeding assets met meerdere child assets_{environment.value[0]}.xlsx'
    output_file = input_file
    df = pd.read_excel(io=input_file, sheet_name='Kast', usecols=["uuid", "type", "naam"], header=0)

    df["toezichter"] = None
    df["ls"] = None
    df["lsdeel"] = None
    df["dnb_laagspanning"] = None
    df["energiemeter"] = None
    df["forfait"] = None
    df["log_info"] = None

    for idx, df_row in df.iterrows():
        asset = eminfra_client.get_asset_by_id(asset_id=df_row["uuid"])
        logging.info(f'Processing asset ({idx}): {asset.uuid}: {asset.naam}')
        df.at[idx, "toezichter"] = get_toezichter_naam(eminfra_client=eminfra_client, asset=asset)

        # Kast: Bevestiging
        if asset.type.uri == INSTALLATIE_TYPES["KAST"]:
            bijhorende_assets = eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid,
                                                                         relatie=RelatieEnum.GEEFTBEVESTIGINGAAN)
        # LS: HoortBij
        if asset.type.uri == INSTALLATIE_TYPES["LS"]:
            bijhorende_assets = eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid,
                                                                         relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
        df.at[idx, "ls"] = len(filter_assets(assets=bijhorende_assets, type_uri=INSTALLATIE_TYPES["LS"]))
        df.at[idx, "lsdeel"] = len(filter_assets(assets=bijhorende_assets, type_uri=INSTALLATIE_TYPES["LSDEEL"]))
        df.at[idx, "dnb_laagspanning"] = len(filter_assets(assets=bijhorende_assets,
                                                          type_uri=ONDERDEEL_TYPES["DNB_LAAG"]))
        df.at[idx, "energiemeter"] = len(filter_assets(assets=bijhorende_assets, type_uri=ONDERDEEL_TYPES["EM_DNB"]))
        df.at[idx, "forfait"] = len(filter_assets(assets=bijhorende_assets, type_uri=ONDERDEEL_TYPES["FORFAIT"]))
        df.at[idx, "log_info"] = extract_info(asset_list=bijhorende_assets)

    with pd.ExcelWriter(output_file, mode='a', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Kast_extra_info', index=False, freeze_panes=[1, 1])
