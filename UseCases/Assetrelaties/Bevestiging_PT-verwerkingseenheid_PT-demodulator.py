from datetime import datetime
from API.eminfra.EMInfraDomain import KenmerkTypeEnum
from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation, create_relation, is_valid_relation
from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_model.OtlmowModel.Classes.Onderdeel.Bevestiging import Bevestiging

print(
    """"
    Aanmaken van Bevestiging-Relaties tussen de PT-Verwerkingseenheid en PT-Demodulator in een DAVIE-conform bestand.
    """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path


def read_RSA_report_as_dataframe(filepath: Path, usecols=None):
    """Read RSA-report as input into a DataFrame."""
    if usecols is None:
        usecols = ["uuid"]
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=usecols)
    df_assets = df_assets.dropna(subset=usecols)     # filter rows with NaN in specific columns
    return df_assets


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # Read input report
    df_assets = read_RSA_report_as_dataframe(
        filepath=Path().home() / 'Downloads' / 'Assetrelaties' / '[RSA] Bevestigingsrelatie bestaat tussen PTVerwerkingseenheid en PTDemodulatoren.xlsx'
        , usecols=["uuid_PT-verwerkingseenheid", "uuid_PT-demodulator",
                   "afstand_tussen_verwerkingseenheid_en_demodulator"]
    )

    bevestiging_relaties = []
    for idx, asset in df_assets.iterrows():
        # Get kenmerken
        kenmerken = eminfra_client.kenmerk_service.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"))
        kenmerk_bevestiging = eminfra_client.kenmerk_service.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"),
                                                                           naam=KenmerkTypeEnum.BEVESTIGD_AAN)

        # Query asset
        relatieTypeId = '3ff9bf1c-d852-442e-a044-6200fe064b20'
        bestaande_relaties = eminfra_client.relatie_service.search_relaties_generator(
            asset_uuid=asset.get("uuid_PT-verwerkingseenheid"), kenmerktype_id=kenmerk_bevestiging.type.get("uuid"),
            relatietype_id=relatieTypeId)

        # Query asset-relaties. Als de relatie al bestaat, continue
        if next(bestaande_relaties, None):
            print(
                f'''Bevestiging-relatie reeds bestaande tussen PT-Verwerkingseenheid ({asset.get(
                    "uuid_PT-verwerkingseenheid")}) en PT-Demodulator ({asset.get("uuid_PT-demodulator")})''')
            continue

        # Genereer relatie volgens het OTLMOW-model
        nieuwe_relatie = create_relation(
            relation_type=Bevestiging()
            , source_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTVerwerkingseenheid'
            , source_uuid=asset.get("uuid_PT-verwerkingseenheid")
            , target_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTRegelaar'
            , target_uuid=asset.get("uuid_PT-demodulator")
        )
        nieuwe_relatie.isActief = True
        nieuwe_relatie.assetId.identificator = f'Bevestiging_{asset.get("uuid_PT-verwerkingseenheid")}_{asset.get("uuid_PT-demodulator")}'
        bevestiging_relaties.append(nieuwe_relatie)

    ######################################
    ### Wegschrijven van de OTL-data naar een DAVIE-conform bestand.
    ######################################
    if bevestiging_relaties:
        filepath = Path().home() / 'Downloads' / 'Assetrelaties' / f'BevestigingRelatie_PT-verwerkingseenheid_PT-demodulator_{datetime.now().strftime("%Y%m%d")}.xlsx'
        OtlmowConverter.from_objects_to_file(
            file_path=filepath
            , sequence_of_objects=bevestiging_relaties
        )
        print(f"DAVIE-file weggeschreven naar:\n\t{filepath}")
