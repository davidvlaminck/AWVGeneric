import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re

from API.EMInfraDomain import OperatorEnum, BoomstructuurAssetTypeEnum, \
    AssetDTOToestand, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, ExpressionDTO, LogicalOpEnum, \
    AssetDTO, EigenschapValueDTO
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

class AssetType(Enum):
    AB = 'Afstandsbewaking'
    CAMERA = 'Camera'
    CAMGROEP = 'CAMGroep'
    DNBHOOGSPANNING = 'DNBHOOGSPANNING'
    ENERGIEMETERDNB = 'ENERGIEMETERDNB'
    GALGPAAL = 'GALGPAAL'
    HS = 'HS'
    HSCABINE = 'HSCabine'
    HSDEEL = 'HSDeel'
    INSTALLATIE = 'Installatie'
    IP = 'IP'
    LSDEEL = 'LSDeel'
    MIVLVE = 'MIVLVE'
    MPT = 'Meetpunt'
    POORT = 'Poort'
    RSSBORD = 'RSSBord'
    RSSGROEP = 'RSSGroep'
    RVMSBORD = 'RVMSBord'
    RVMSGROEP = 'RVMSGroep'
    SEGC = 'Segmentcontroller'
    SEINBRUGDVM = 'SeinbrugDVM'
    SWITCH = 'SWITCH'
    TT = 'Teletransmissieverbinding'
    WEGKANTKAST = 'Wegkantkast'
    WVGROEP = 'Wegverlichtingsgroep'
    WVLICHTMAST = 'Wegverlichtingsmast'

class RelatieType(Enum):
    BEVESTIGING = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging'
    VOEDT = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt'
    STURING = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing'
    HOORTBIJ = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij'

@dataclass
class RelatieInfo:
    uri: RelatieType
    bronAsset_uuid: str | None = None
    doelAsset_uuid: str | None = None

@dataclass
class AssetInfo:
    asset_type: AssetType
    column_uuid: str | None = None
    column_typeURI: str | None = None
    column_name: str | None = None

@dataclass
class ParentAssetInfo:
    parent_asset_type: BoomstructuurAssetTypeEnum
    column_parent_uuid: str | None = None
    column_parent_name: str | None = None

@dataclass
class EigenschapInfo:
    eminfra_eigenschap_name: str
    column_eigenschap_name: str

class BypassProcessor:
    def __init__(self
                 , environment: Environment = Environment.TEI
                 , settings_path: Path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
                 , eDelta_dossiernummer: str = 'INTERN-095'
                 , input_path_componentenlijst: Path = Path(
                __file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250507.xlsx'
                 , output_excel_path: Path = Path(
                __file__).resolve().parent / 'data' / 'output' / f'lantis_bypass_{datetime.now().strftime(format="%Y-%m-%d")}.xlsx'
                 , startdatum_bestekkoppeling: datetime = datetime(2024, 9, 1)
                 ):
        self.setup_logging()

        self.excel_file = input_path_componentenlijst
        self.environment = environment
        logging.info(f'Omgeving: {self.environment.name}')

        self.settings_path = settings_path
        logging.info(f'settings_path: {self.settings_path}')

        self.eminfra_client = EMInfraClient(env=self.environment, auth_type=AuthType.JWT,
                                            settings_path=self.settings_path)
        logging.info('EM-Infra client initialized')

        self.eDelta_dossiernummer = eDelta_dossiernummer
        logging.info(f'Bestekkoppeling: {self.eDelta_dossiernummer}')

        self.start_datetime = startdatum_bestekkoppeling
        logging.info(f'Startdatum van de bestekkoppeling luidt: {self.start_datetime}')

        self.excel_file = input_path_componentenlijst
        logging.info(f"Excel file wordt ingelezen en gevalideerd: {self.excel_file}")

        self.output_excel_path = output_excel_path
        if not Path(self.output_excel_path).exists():
            with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
                metadata_df = pd.DataFrame({
                    "Field": ["Author", "Timestamp", "Description"],
                    "Value": [
                        'Dries Verdoodt - dries.verdoodt@mow.vlaanderen.be',
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'Lantis Bypass | aanmaken van nieuwe assets in EM-infra'
                    ]
                })
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)

        logging.info(f'Output file path: {self.output_excel_path}')

        self.setup_mapping_dict_typeURI()
        self.setup_mapping_dict_assettype()

    def setup_mapping_dict_typeURI(self):
        self.typeURI_mapping_dict = {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Wegkantkast": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HSCabine": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy"
            , "lgc:installatie#HS": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS"
            , "lgc:installatie#HSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel"
            , "lgc:installatie#LS": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS"
            , "lgc:installatie#LSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB"
            , "IP": "https://lgc.data.wegenenverkeer.be/ns/installatie#IP"
            , "Switch": "https://lgc.data.wegenenverkeer.be/ns/installatie#Switch"
            , "TT": "https://lgc.data.wegenenverkeer.be/ns/installatie#TT"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Segmentcontroller": "https://lgc.data.wegenenverkeer.be/ns/installatie#SegC"
            , "lgc:installatie#WV": "https://lgc.data.wegenenverkeer.be/ns/installatie#WV"
            , "lgc:installatie#IP": "https://lgc.data.wegenenverkeer.be/ns/installatie#IP"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule": "https://lgc.data.wegenenverkeer.be/ns/installatie#MIVLVE"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus": "https://lgc.data.wegenenverkeer.be/ns/installatie#Mpt"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS": "https://lgc.data.wegenenverkeer.be/ns/installatie#RSSBord"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS": "https://lgc.data.wegenenverkeer.be/ns/installatie#RVMS"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS": "https://lgc.data.wegenenverkeer.be/ns/installatie#VMS"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera": "https://lgc.data.wegenenverkeer.be/ns/installatie#CameraLegacy"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug": "https://lgc.data.wegenenverkeer.be/ns/installatie#SeinbrugDVM"
            ,
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal"
        }

    def setup_mapping_dict_assettype(self):
        self.assettype_mapping_dict = {
            "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast": "10377658-776f-4c21-a294-6c740b9f655e",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Wegkantkast": "c3601915-3b66-4bde-9728-25c1bbf2f374",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy": "1cf24e76-5bf3-44b0-8332-a47ab126b87e",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HSCabine": "d76cbedd-5488-428c-a221-fe0bc8f74fa2",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#HS": "46dcd9b1-f660-4c8c-8e3e-9cf794b4de75",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel": "a9655f50-3de7-4c18-aa25-181c372486b1",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#LS": "80fdf1b4-e311-4270-92ba-6367d2a42d47",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel": "b4361a72-e1d5-41c5-bfcc-d48f459f4048",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning": "8e9307e2-4dd6-4a46-a298-dd0bc8b34236",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB": "ca3ae27f-c611-4761-97d1-d9766dd30e0a",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning": "b4ee4ea9-edd1-4093-bce1-d58918aee281",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#Switch": "e77befed-4530-4d57-bdb9-426bdae4775d",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#IP": "5454b9b1-1bf4-4096-a124-1e3aeee725a2",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#SegC": "f625b904-befc-4685-9dd8-15a20b23a58b",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Segmentcontroller": "6c1883d1-7e50-441a-854c-b53552001e5f",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#WV": "55362c2a-be7b-4efc-9437-765b351c8c51",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast": "478add39-e6fb-4b0b-b090-9c65e836f3a0",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast": "4dfad588-277c-480f-8cdc-0889cfaf9c78",
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule": "7f59b64e-9d6c-4ac9-8de7-a279973c9210",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#MIVLVE": "a4c75282-972a-4132-ad72-0d0de09dbdb8",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus": "63b42487-8f07-4d9a-823e-c5a5f3c0aa81",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#Mpt": "dc3db3b7-7aad-4d7f-a788-a4978f803021",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS": "9826b683-02fa-4d97-8680-fbabc91a417f",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#RSSBord": "1496b2fd-0742-44a9-a3b4-e994bd5af8af",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS": "50f7400a-2e67-4550-b135-08cde6f6d64f",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#VMS": "ac837aa9-65bc-4c7c-b1c2-8ec0201a0203",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS": "0515e9bc-1778-43ae-81a9-44df3e2b7c21",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#RVMS": "5b44cb96-3edf-4ef5-bc85-ec4d5c5152a3",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera": "3f98f53a-b435-4a69-af3c-cede1cd373a7",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#CameraLegacy": "f66d1ad1-4247-4d99-80bb-5a2e6331eb96",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug": "40b2e487-f4b8-48a2-be9d-e68263bab75a",
            "https://lgc.data.wegenenverkeer.be/ns/installatie#SeinbrugDVM": "6f66dad8-8290-4d07-8e8b-6add6c7fe723",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal": "615356ae-64eb-4a7d-8f40-6e496ec5b8d7"
        }

    def setup_logging(self):
        logging.basicConfig(filename="logs.log", level=logging.DEBUG,
                            format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
        logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    def import_data(self):
        """
        Import data from Excel into a Pandas dataframe. Validate data. Append attributes (uuid's)

        Initiate global variables to store pandas dataframes

        :return: None
        """
        logging.info('Import data, validate, and prepare the dataframes.')
        self.df_assets_installaties = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                    sheet_name="Wegkantkasten")
        self.df_assets_installaties = self.append_columns(df=self.df_assets_installaties,
                                                          columns=["installatie_uuid", "installatie_naam"])

        self.df_assets_wegkantkasten = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                     sheet_name="Wegkantkasten")
        self.df_assets_wegkantkasten = self.append_columns(df=self.df_assets_wegkantkasten, columns=["asset_uuid", "asset_uuid_lsdeel", "bevestigingsrelatie_uuid_lsdeel", "voedingsrelatie_uuid_lsdeel", "asset_uuid_switch", "bevestigingsrelatie_uuid_switch", "asset_uuid_teletransmissieverbinding"])

        self.df_assets_voeding = self.import_data_as_dataframe(filepath=self.excel_file,
                                                               sheet_name="HSCabines-CC-SC-HS-LS-Switch-WV")
        self.df_assets_voeding_HS_cabine = self.df_assets_voeding.loc[:,
                                           ['HSCabine_Object naam ROCO', 'HSCabine_Object assetId.toegekendDoor',
                                            'HSCabine_Object assetId.identificator', 'HSCabine_UUID Object',
                                            'HSCabine_Object typeURI', 'HSCabine_Positie X (Lambert 72)',
                                            'HSCabine_Positie Y (Lambert 72)', 'HSCabine_Positie Z (Lambert 72)']]
        self.df_assets_voeding_HS_cabine = self.append_columns(df=self.df_assets_voeding_HS_cabine,
                                                               columns=["asset_uuid"])

        self.df_assets_voeding_hoogspanningsdeel = self.df_assets_voeding.loc[:,
                                                   ['Hoogspanningsdeel_HSDeel aanwezig (Ja/Nee)',
                                                    'Hoogspanningsdeel_Naam HSDeel',
                                                    'Hoogspanningsdeel_HSDeel lgc:installatie',
                                                    'Hoogspanningsdeel_UUID HSDeel',
                                                    'Bevestigingsrelatie HSDeel_Bevestigingsrelatie assetId.identificator',
                                                    'Bevestigingsrelatie HSDeel_Bevestigingsrelatie typeURI',
                                                    'Bevestigingsrelatie HSDeel_UUID Bevestigingsrelatie']]
        self.df_assets_voeding_hoogspanningsdeel = self.append_columns(df=self.df_assets_voeding_hoogspanningsdeel,
                                                                       columns=["asset_uuid",
                                                                                "bevestigingsrelatie_uuid"])

        self.df_assets_voeding_laagspanningsdeel = self.df_assets_voeding.loc[:,
                                                   ['Laagspanningsdeel_LSDeel aanwezig (Ja/Nee)',
                                                    'Laagspanningsdeel_Naam LSDeel',
                                                    'Laagspanningsdeel_LSDeel lgc:installatie',
                                                    'Laagspanningsdeel_UUID LSDeel',
                                                    'Bevestigingsrelatie LSDeel_Bevestigingsrelatie assetId.identificator',
                                                    'Bevestigingsrelatie LSDeel_Bevestigingsrelatie typeURI',
                                                    'Bevestigingsrelatie LSDeel_UUID Bevestigingsrelatie',
                                                    'Voedingsrelatie HSDeel naar LSDeel_Voedingsrelatie assetId.identificator',
                                                    'Voedingsrelatie HSDeel naar LSDeel_Voedingsrelatie typeURI',
                                                    'Voedingsrelatie HSDeel naar LSDeel_UUID Voedingsrelatie bronAsset',
                                                    'Voedingsrelatie HSDeel naar LSDeel_UUID Voedingsrelatie']]
        self.df_assets_voeding_laagspanningsdeel = self.append_columns(df=self.df_assets_voeding_laagspanningsdeel,
                                                                       columns=["asset_uuid",
                                                                                "bevestigingsrelatie_uuid",
                                                                                "voedingsrelatie_uuid"])

        self.df_assets_voeding_hoogspanning = self.df_assets_voeding.loc[:,
                                              ['Hoogspanning_HS aanwezig (Ja/Nee)', 'Hoogspanning_Naam HS',
                                               'Hoogspanning_HS lgc:installatie', 'Hoogspanning_UUID HS',
                                               'Bevestigingsrelatie HS_Bevestigingsrelatie assetId.identificator',
                                               'Bevestigingsrelatie HS_Bevestigingsrelatie typeURI',
                                               'Bevestigingsrelatie HS_UUID Bevestigingsrelatie']]
        self.df_assets_voeding_hoogspanning = self.append_columns(df=self.df_assets_voeding_hoogspanning,
                                                                  columns=["asset_uuid", "bevestigingsrelatie_uuid"])

        self.df_assets_voeding_DNBHoogspanning = self.df_assets_voeding.loc[:,
                                                 ['DNBHoogspanning_Object assetId.identificator',
                                                  'DNBHoogspanning_UUID Object', 'DNBHoogspanning_Object typeURI',
                                                  'DNBHoogspanning_eanNummer', 'DNBHoogspanning_referentieDNB',
                                                  'HoortBij Relatie voor DNBHoogspanning_HoortBij assetId.identificator',
                                                  'HoortBij Relatie voor DNBHoogspanning_HoortBij typeURI',
                                                  'HoortBij Relatie voor DNBHoogspanning_UUID HoortBijrelatie']]
        self.df_assets_voeding_DNBHoogspanning = self.append_columns(df=self.df_assets_voeding_DNBHoogspanning,
                                                                     columns=["asset_uuid", "hoortbijrelatie_uuid"])

        self.df_assets_voeding_energiemeter_DNB = self.df_assets_voeding.loc[:,
                                                  ['EnergiemeterDNB_Object assetId.identificator',
                                                   'EnergiemeterDNB_UUID Object', 'EnergiemeterDNB_Object typeURI',
                                                   'EnergiemeterDNB_meternummer',
                                                   'HoortBij Relatie voor EnergiemeterDNB_HoortBij assetId.identificator',
                                                   'HoortBij Relatie voor EnergiemeterDNB_HoortBij typeURI',
                                                   'HoortBij Relatie voor EnergiemeterDNB_UUID HoortBijrelatie']]
        self.df_assets_voeding_energiemeter_DNB = self.append_columns(df=self.df_assets_voeding_energiemeter_DNB,
                                                                      columns=["asset_uuid", "hoortbijrelatie_uuid"])

        self.df_assets_voeding_segmentcontroller = self.df_assets_voeding.loc[:,
                                                   ['Segmentcontroller_Naam SC', 'Segmentcontroller_SC TypeURI',
                                                    'Segmentcontroller_UUID SC']]
        self.df_assets_voeding_segmentcontroller = self.append_columns(df=self.df_assets_voeding_segmentcontroller,
                                                                       columns=["asset_uuid"])

        self.df_assets_voeding_wegverlichting = self.df_assets_voeding.loc[:,
                                                ['Wegverlichtingsgroep_WV aanwezig (Ja/Nee)',
                                                 'Wegverlichtingsgroep_Naam WV',
                                                 'Wegverlichtingsgroep_WV lgc:installatie',
                                                 'Wegverlichtingsgroep_UUID WV']]
        self.df_assets_voeding_wegverlichting = self.append_columns(df=self.df_assets_voeding_wegverlichting,
                                                                    columns=["asset_uuid"])

        self.df_assets_voeding_switch = self.df_assets_voeding.loc[:, ['Switch gegevens_Switch aanwezig (Ja/Nee)',
                                                                       'Switch gegevens_Object assetId.toegekendDoor',
                                                                       'Switch gegevens_Object assetId.identificator',
                                                                       'Switch gegevens_UUID switch',
                                                                       'Switch gegevens_Aantal poorten',
                                                                       'Switch gegevens_Glasvezellus']]
        self.df_assets_voeding_switch = self.append_columns(df=self.df_assets_voeding_switch, columns=["asset_uuid"])

        self.df_assets_openbare_verlichting = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                            sheet_name="Openbare verlichting")
        self.df_assets_openbare_verlichting = self.append_columns(df=self.df_assets_openbare_verlichting,
                                                                  columns=["asset_uuid", "voedingsrelatie_uuid"])

        self.df_assets_mivlve = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="MIVLVE")
        self.df_assets_mivlve = self.append_columns(df=self.df_assets_mivlve,
                                                    columns=["asset_uuid", "bevestigingsrelatie_uuid"])

        self.df_assets_mivmeetpunten = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                     sheet_name="MIVMeetpunten")

        self.df_assets_mivmeetpunten = self.append_columns(df=self.df_assets_mivmeetpunten,
                                                           columns=["asset_uuid", "sturingsrelatie_uuid"])
        self.df_assets_RSS_borden = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="RSS-borden")
        self.df_assets_RSS_borden = self.append_columns(df=self.df_assets_RSS_borden,
                                                        columns=["asset_uuid", "hoortbijrelatie_uuid",
                                                                 "bevestigingsrelatie_uuid", "voedingsrelatie_uuid",
                                                                 "sturingsrelatie_uuid"])

        self.df_assets_RVMS_borden = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="(R)VMS-borden")
        self.df_assets_RVMS_borden = self.append_columns(df=self.df_assets_RVMS_borden,
                                                         columns=["asset_uuid", "hoortbijrelatie_uuid",
                                                                  "bevestigingsrelatie_uuid",
                                                                  "voedingsrelatie_uuid", "sturingsrelatie_uuid"])

        self.df_assets_cameras = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="Cameras")
        self.df_assets_cameras = self.append_columns(df=self.df_assets_cameras,
                                                     columns=["asset_uuid", "voedingsrelatie_uuid",
                                                              "bevestigingsrelatie_uuid", "sturingsrelatie_uuid"])

        self.df_assets_portieken_seinbruggen = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                             sheet_name="Portieken-Seinbruggen")
        self.df_assets_portieken_seinbruggen = self.append_columns(df=self.df_assets_portieken_seinbruggen,
                                                                   columns=["asset_uuid"])

        self.df_assets_galgpaal = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="Galgpaal")
        self.df_assets_galgpaal = self.append_columns(df=self.df_assets_galgpaal, columns=["asset_uuid"])


    def process_installatie(self, df: pd.DataFrame, column_name: str, asset_type: AssetType) -> None:
        logging.info(f'Aanmaken van installaties bij het assettype: {asset_type}')
        for idx, asset_row in df.iterrows():
            asset_row_naam = asset_row.get(column_name)
            installatie_naam = self.construct_installatie_naam(naam=asset_row_naam, asset_type=asset_type)
            df.at[idx, "installatie_naam"] = installatie_naam
            df.at[idx, "installatie_uuid"] = self.create_installatie_if_missing(naam=installatie_naam)

        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'Installaties_{asset_type.value}',
                        columns=["installatie_uuid", "installatie_naam"],
                        index=False, freeze_panes=[1, 1])
        logging.info(f'Installaties bij het assettype {asset_type} aangemaakt')

    # Helper function for lookups
    def _search_parent_asset_by_uuid(self, uuid: str):
        if parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.ASSET:
            return next(self.eminfra_client.search_asset_by_uuid(asset_uuid=uuid), None)
        elif parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.BEHEEROBJECT:
            return self.eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=uuid)
        return None

    def _search_parent_asset_by_name(self, name: str):
        if parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.ASSET:
            return next(self.eminfra_client.search_asset_by_name(asset_name=name, exact_search=True), None)
        elif parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.BEHEEROBJECT:
            return next(self.eminfra_client.search_beheerobjecten(naam=name, operator=OperatorEnum.EQ), None)
        return None

    def process_assets(self
                       , df: pd.DataFrame
                       , asset_info: AssetInfo
                       , parent_asset_info: ParentAssetInfo
                       , eigenschap_infos: [EigenschapInfo] = None
                       , relatie_infos: [RelatieInfo] = None) -> None:
        """
        verwerken van een dataframe van assets.

        :param parent_asset_type:
        :param asset_type:
        :param df:
        :param column_uuid:
        :param column_typeURI:
        :param column_name:
        :param column_parent_uuid:
        :param column_parent_name:
        :param relatie_infos: dataclass. bronAsset_uuid and doelAsset_uuid are the names of the dataframe column storing the uuid. uri is the relation-uri
        :return:
        """
        logging.info(f'Aanmaken van assets ... (assettype: {asset_info.asset_type.value}) ')

        if relatie_infos is None:
            relatie_infos = []
        if eigenschap_infos is None:
            eigenschap_infos = []

        df_output_columns = set()
        df_output_columns.add(asset_info.column_name)

        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get(asset_info.column_uuid)
            if asset_info.column_typeURI.startswith('https://') or asset_info.column_typeURI.startswith('lgc:'):
                asset_row_typeURI = asset_info.column_typeURI
            else:
                asset_row_typeURI = asset_row.get(asset_info.column_typeURI)
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get(asset_info.column_name)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            # Default to None
            parent_asset = None

            # Try UUID lookup first
            if parent_asset_info.column_parent_uuid:
                parent_uuid = asset_row.get(parent_asset_info.column_parent_uuid)
                if parent_uuid:
                    parent_asset = self._search_parent_asset_by_uuid(parent_uuid)

            # Try name-based lookup
            elif parent_asset_info.column_parent_name:
                parent_name = asset_row.get(parent_asset_info.column_parent_name)
                if parent_name:
                    parent_asset = self._search_parent_asset_by_name(parent_name)

            # Fallback to constructed name if needed. Name of the parent constructed via name of the asset itself.
            else:
                fallback_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=asset_info.asset_type)
                parent_asset = next(
                    self.eminfra_client.search_beheerobjecten(naam=fallback_name, actief=True,
                                                              operator=OperatorEnum.EQ),
                    None
                )

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                # todo: uitzoeken hoe je een asset kan aanpasen, met parent als optionele parameter.
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=parent_asset_info.parent_asset_type)


                df.at[idx, "asset_uuid"] = asset.uuid
                df_output_columns.add("asset_uuid")

                # Aanmaken van eigenschappen
                for eigenschap_info in eigenschap_infos:
                    self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand=eigenschap_info.eminfra_eigenschap_name,
                                           eigenschapwaarde_nieuw=asset_row.get(eigenschap_info.column_eigenschap_name))

                # Aanmaken van relaties
                for relatie_info in relatie_infos:
                    relatie_descriptive_naam = relatie_info.uri.value.split('#')[-1]
                    bronAsset_uuid = asset_row.get(relatie_info.bronAsset_uuid, asset.uuid)
                    doelAsset_uuid = asset_row.get(relatie_info.doelAsset_uuid, asset.uuid)
                    relatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                  doelAsset_uuid=doelAsset_uuid,
                                                                  relatie_naam=relatie_info.uri.value)
                    # append relatie_uuid to the dataframe
                    df.at[idx, f'relatie_uuid_{relatie_descriptive_naam}'] = relatie_uuid
                    df_output_columns.add(f'relatie_uuid_{relatie_descriptive_naam}')

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'{asset_info.asset_type.value}',
                        columns=list(df_output_columns).sort(), index=False, freeze_panes=[1, 1])
        logging.info(f'Assets aangemaakt (assettype: {asset_info.asset_type.value})')


    def process_wegkantkasten(self, df: pd.DataFrame):
        logging.info('Aanmaken van Wegkantkasten onder installaties')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Wegkantkast_UUID Object")
            asset_row_typeURI = asset_row.get("Wegkantkast_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Wegkantkast_Object assetId.identificator")

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.WEGKANTKAST)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid_lsdeel"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Wegkantkasten',
                        columns=["Wegkantkast_Object assetId.identificator", "asset_uuid_lsdeel"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Wegkantkasten aangemaakt')

    def process_wegkantkasten_lsdeel(self, df: pd.DataFrame):
        logging.info('Aanmaken van Wegkantkasten_LSDeel')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Laagspanningsdeel_UUID LSDeel")
            asset_row_typeURI = 'lgc:installatie#LSDeel'
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, None)
            asset_row_name = asset_row.get("Laagspanningsdeel_Naam LSDeel")
            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.LSDEEL)
            parent_asset = next(
                self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                          operator=OperatorEnum.EQ), None)

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

                # Bevestiging-relatie
                doelAsset_uuid = asset_row.get("Wegkantkast_UUID Object")
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                                 doelAsset_uuid=doelAsset_uuid,
                                                                                 relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
                # Voeding-relatie
                bronAsset_uuid = asset_row.get("Voedingsrelatie (oorsprong)_UUID Voedingsrelatie bronAsset")
                doelAsset_uuid = asset_row.get("Laagspanningsdeel_UUID LSDeel")
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                             doelAsset_uuid=doelAsset_uuid,
                                                                             relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')

                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid_lsdeel"] = asset.uuid
                df.at[idx, "bevestigingsrelatie_uuid_lsdeel"] = bevestigingsrelatie_uuid
                df.at[idx, "voedingsrelatie_uuid_lsdeel"] = voedingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Wegkantkasten_LSDeel',
                        columns=["Laagspanningsdeel_Naam LSDeel", "asset_uuid_lsdeel", "bevestigingsrelatie_uuid_lsdeel", "voedingsrelatie_uuid_lsdeel"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Wegkantkasten_LSDeel aangemaakt')


    def process_wegkantkasten_switch(self, df: pd.DataFrame):
        logging.info('Aanmaken van Wegkantkasten_Switch')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Switch gegevens_UUID switch")
            asset_row_typeURI = 'Switch'
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, None)
            asset_row_name = asset_row.get("Switch gegevens_Object assetId.identificator")
            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.IP)
            parent_asset = next(
                self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                          operator=OperatorEnum.EQ), None)

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

                # Bevestiging-relatie
                bronAsset_uuid = asset_row.get("Switch gegevens_UUID Switch")
                doelAsset_uuid = asset_row.get("Wegkantkast_UUID Object")
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                                 doelAsset_uuid=doelAsset_uuid,
                                                                                 relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')

                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid_switch"] = asset.uuid
                df.at[idx, "bevestigingsrelatie_uuid_switch"] = bevestigingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Wegkantkasten_Switch',
                        columns=["Switch gegevens_Object assetId.identificator", "asset_uuid_switch", "bevestigingsrelatie_uuid_switch"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Wegkantkasten_Switch aangemaakt')

    def process_wegkantkasten_teletransmissieverbinding(self, df: pd.DataFrame):
        logging.info('Aanmaken van Wegkantkasten_Teletransmissieverbinding')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Switch gegevens_UUID Teletransmissieverbinding")
            asset_row_typeURI = 'TT'
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, None)
            asset_row_name = asset_row.get("Switch gegevens_Naam Teletransmissieverbinding")
            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.TT)
            parent_asset = next(
                self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                          operator=OperatorEnum.EQ), None)

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid_teletransmissieverbinding"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Wegkantkasten_TT',
                        columns=["Switch gegevens_Naam Teletransmissieverbinding", "asset_uuid_teletransmissieverbinding"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Wegkantkasten_Teletransmisseiverbinding aangemaakt')


    def process_voeding_HS_cabine(self, df: pd.DataFrame):
        # HSCabine
        logging.info('Aanmaken van HSCabine onder installaties')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("HSCabine_UUID Object")
            asset_row_typeURI = asset_row.get("HSCabine_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("HSCabine_Object assetId.identificator")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.HSCABINE)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)






                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='HSCabine', columns=["HSCabine_Object assetId.identificator", "asset_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('HSCabine aangemaakt')

    def process_voeding_hoogspanningsdeel(self, df: pd.DataFrame):
        logging.info('Aanmaken van Hoogspanningsdeel onder Hoogspanning Cabine')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Hoogspanningsdeel_UUID HSDeel")
            asset_row_typeURI = asset_row.get("Hoogspanningsdeel_HSDeel lgc:installatie")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Hoogspanningsdeel_Naam HSDeel")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.HSDEEL)
            parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Bevestiging-relatie
                doelAsset_uuid = df.loc[idx, "HSCabine_UUID Object"]
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Hoogspanningsdeel', columns=["Hoogspanningsdeel_Naam HSDeel", "asset_uuid", "bevestigingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Hoogspanningsdeel aangemaakt')

    def process_voeding_laagspanningsdeel(self, df: pd.DataFrame):
        logging.info('Aanmaken van Laagspanningsdeel onder Hoogspanning Cabine')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Laagspanningsdeel_UUID LSDeel")
            asset_row_typeURI = asset_row.get("Laagspanningsdeel_LSDeel lgc:installatie")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Laagspanningsdeel_Naam LSDeel")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.LSDEEL)
            parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Bevestiging-relatie
                doelAsset_uuid = df.loc[idx, "HSCabine_UUID Object"]
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
                # Voeding-relatie
                bronAsset_uuid = df.loc[idx, "Hoogspanningsdeel_UUID HSDeel"]
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voeding')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Laagspanningsdeel',
                        columns=["Laagspanningsdeel_Naam LSDeel", "asset_uuid", "bevestigingsrelatie_uuid",
                                 "voedingsrelatie_uuid"], index=False, freeze_panes=[1, 1])
        logging.info('Laagspanningsdeel aangemaakt')

    def process_voeding_hoogspanning(self, df: pd.DataFrame):
        logging.info('Aanmaken van Hoogspanning onder Hoogspanning Cabine')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Hoogspanning_UUID HSDeel")
            asset_row_typeURI = asset_row.get("Hoogspanning_HS lgc:installatie")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Hoogspanning_Naam HS")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.HS)
            parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Bevestiging-relatie
                doelAsset_uuid = df.loc[idx, "HSCabine_UUID Object"]
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Hoogspanning',
                        columns=["Hoogspanningsdeel_Naam HSDeel", "asset_uuid", "bevestigingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Hoogspanning aangemaakt')

    def process_voeding_DNBHoogspanning(self, df: pd.DataFrame):
        logging.info('Aanmaken van DNBHoogspanning (OTL)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("DNBHoogspanning_UUID Object")
            asset_row_typeURI = asset_row.get("DNBHoogspanning_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("DNBHoogspanning_Object assetId.identificator")
            asset_row_parent_uuid = df.loc[idx, "Hoogspanning_UUID HS"]
            if not pd.isna(asset_row_parent_uuid):
                parent_asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_parent_uuid), None)
            else:
                parent_asset = None



            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)
                # reorganize OTL-asset in tree-structure
                self.eminfra_client.reorganize_beheerobject(parentAsset=parent_asset, childAsset=asset, parentType=BoomstructuurAssetTypeEnum.ASSET)


                # Update eigenschapwaarden
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='eanNummer', eigenschapwaarde_nieuw=asset_row.get("DNBHoogspanning_eanNummer"))
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='referentieDNB', eigenschapwaarde_nieuw=asset_row.get("DNBHoogspanning_referentieDNB"))

                # Hoortbij-relatie
                hoortbijrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                      doelAsset_uuid=parent_asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "hoortbijrelatie_uuid"] = hoortbijrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='DNBHoogspanning',
                        columns=["DNBHoogspanning_Object assetId.identificator", "asset_uuid", "hoortbijrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('DNBHoogspanning aangemaakt')

    def process_voeding_energiemeter_DNB(self, df: pd.DataFrame):
        logging.info('Aanmaken van EnergiemeterDNB (OTL)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("EnergiemeterDNB_UUID Object")
            asset_row_typeURI = asset_row.get("EnergiemeterDNB_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("EnergiemeterDNB_Object assetId.identificator")
            asset_row_parent_uuid = df.loc[idx, "Hoogspanning_UUID HS"]
            if not pd.isna(asset_row_parent_uuid):
                parent_asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_parent_uuid), None)
            else:
                parent_asset = None

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)

                # reorganize OTL-asset in tree-structure
                self.eminfra_client.reorganize_beheerobject(parentAsset=parent_asset, childAsset=asset, parentType=BoomstructuurAssetTypeEnum.ASSET)


                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='meternummer', eigenschapwaarde_nieuw=asset_row.get("EnergiemeterDNB_meternummer"))

                # Hoortbij-relatie
                hoortbijrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                      doelAsset_uuid=parent_asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "hoortbijrelatie_uuid"] = hoortbijrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Energiemeter DNB',
                        columns=["EnergiemeterDNB_Object assetId.identificator", "asset_uuid", "hoortbijrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('EnergiemeterDNB aangemaakt')

    def process_voeding_segmentcontroller(self, df: pd.DataFrame):
        logging.info('Aanmaken van SegmentController (OTL)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Segmentcontroller_UUID SC")
            asset_row_typeURI = asset_row.get("Segmentcontroller_SC TypeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Segmentcontroller_Naam SC")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.SEGC)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)




                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Segmentcontroller', columns=["Segmentcontroller_Naam SC", "asset_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Segmentcontroller aangemaakt')

    def process_voeding_wegverlichting(self, df: pd.DataFrame):
        # Wegverlichtingsgroep
        logging.info('Aanmaken van Wegverlichtingsgroep')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Wegverlichtingsgroep_UUID WV")
            asset_row_typeURI = asset_row.get("Wegverlichtingsgroep_WV lgc:installatie")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Wegverlichtingsgroep_Naam WV")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.WVGROEP)
            # todo pas de zoekopdracht naar de parent aan naar ofwel een Beheerobject, ofwel een Asset
            # parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)




                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Wegverlichting', columns=["Wegverlichtingsgroep_Naam WV", "asset_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Wegverlichtingsgroep aangemaakt')

    def process_voeding_switch(self, df: pd.DataFrame):
        logging.info('Aanmaken Switch (IP-Netwerkapparatuur)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Switch gegevens_UUID switch")
            asset_row_typeURI = 'Switch'
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, None)
            asset_row_name = asset_row.get("Switch gegevens_Object assetId.identificator")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.IP)
            # todo pas de zoekopdracht naar de parent aan naar ofwel een Beheerobject, ofwel een Asset
            # parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)




                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Switch', columns=["Switch gegevens_Object assetId.identificator", "asset_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Switch (IP-Netwerkapparatuur) aangemaakt')

    def process_openbare_verlichting(self, df: pd.DataFrame):
        # Openbare verlichting
        logging.info('Aanmaken Openbare verlichting (WVLichtmast)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("WVLichtmast_UUID Object")
            asset_row_typeURI = asset_row.get("WVLichtmast_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("WVLichtmast_Object assetId.identificator")
            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.WVLICHTMAST)
            # todo pas de zoekopdracht naar de parent aan naar ofwel een Beheerobject, ofwel een Asset
            # parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)




                # Voeding-relatie
                bronAsset_uuid = asset_row.get('Voedingsrelaties_UUID Voedingsrelatie bronAsset')
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Openbare verlichting',
                        columns=["WVLichtmast_Object assetId.identificator", "asset_uuid", "voedingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('Openbare verlichting (WVLichtmast) aangemaakt')

    def process_mivlve(self, df: pd.DataFrame):
        logging.info('Aanmaken van MIVLVE onder Wegkantkasten')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("LVE_UUID Object")
            asset_row_typeURI = asset_row.get("LVE_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("LVE_Object assetId.identificator")
            asset_row_parent_name = asset_row.get("Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator")
            parent_asset = next(
                self.eminfra_client.search_assets(query_dto=self.build_query_MIVLVE(naam=asset_row_parent_name)), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)



                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='type MIV installatie', eigenschapwaarde_nieuw=asset_row.get("LVE_Type"))

                # Voeding-relatie
                bronAsset_uuid = asset_row.get('Voedingsrelaties_UUID Voedingsrelatie bronAsset')
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                          doelAsset_uuid=asset.uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')


                # Bevestiging-relatie
                doelAsset_uuid = asset_row.get('Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')

                # Sturing-relatie
                # TODO: controleer of the bron van de sturingsrelatie effectief de Switch is.
                bronAsset_uuid = asset_row.get('Netwerkgegevens_UUID Switch')
                sturingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                          doelAsset_uuid=asset.uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing')




                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid
                df.at[idx, "sturingsrelatie_uuid"] = sturingsrelatie_uuid

        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='MIVLVE',
                        columns=["LVE_Object assetId.identificator", "asset_uuid", "voedingsrelatie_uuid", "bevestigingsrelatie_uuid", "sturingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('MIVLVE aangemaakt')

    def process_mivmeetpunten(self, df: pd.DataFrame):
        # Aanmaken van de MIVMeetpunten
        logging.info('Aanmaken van MIVMeetpunten onder MIVLVE')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Meetpunt_UUID Object")
            asset_row_typeURI = asset_row.get("Meetpunt_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Meetpunt_Object assetId.identificator")

            asset_row_parent_name = asset_row.get("Sturingsrelaties_Sturingsrelatie bron AssetId.identificator")
            parent_asset = next(
                self.eminfra_client.search_assets(query_dto=self.build_query_MIVMeetpunten(naam=asset_row_parent_name)),
                None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)

                if asset is None:
                    logging.critical('Asset werd niet aangemaakt')


                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='aansluiting', eigenschapwaarde_nieuw=asset_row.get("Meetpunt_Aansluiting"))
                # self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='', eigenschapwaarde_nieuw=asset_row.get("Meetpunt_Formaat"))
                # self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='', eigenschapwaarde_nieuw=asset_row.get("Meetpunt_Laag"))
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='uitslijprichting', eigenschapwaarde_nieuw=asset_row.get("Meetpunt_Uitslijprichting"))
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='wegdek', eigenschapwaarde_nieuw=asset_row.get("Meetpunt_Wegdek"))



                # Sturing-relatie
                bronAsset_uuid = asset_row.get('Sturingsrelaties_UUID Sturingsrelatie bronAsset')
                sturingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "sturingsrelatie_uuid"] = sturingsrelatie_uuid

        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='MIVMeetpunten',
                        columns=["Meetpunt_Object assetId.identificator", "asset_uuid",
                                 "sturingsrelatie_uuid"], index=False, freeze_panes=[1, 1])
        logging.info('MIVMeetpunten aangemaakt')

    def process_cameras(self, df: pd.DataFrame):
        logging.info('Aanmaken Cameras')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Camera_UUID Object")
            asset_row_typeURI = asset_row.get("Camera_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Camera_Object assetId.identificator")
            # Camera via diens Bevestigingsrelatie op de juiste plaats in de boomstructuur plaatsen
            parent_asset_uuid = asset_row.get("Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset")
            if not pd.isna(parent_asset_uuid):
                parent_asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=parent_asset_uuid), None)
            else:
                parent_asset = None

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')
            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Update eigenschappen
                asset_row_type_camera = asset_row.get("Camera_Type Camera")
                if asset_row_type_camera == 'PTZ':
                    self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='isPtz', eigenschapwaarde_nieuw=True)
                elif asset_row_type_camera == 'AID':
                    self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='heeftAid', eigenschapwaarde_nieuw=True)
                # self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='', eigenschapwaarde_nieuw=asset_row.get("Camera_Kijkrichting"))



                # Relaties
                # Voedingsrelatie
                bronAsset_uuid = asset_row.get('Voedingsrelaties_UUID Voedingsrelatie bronAsset')
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')
                # Bevestigingsrelatie
                doelAsset_uuid = asset_row.get('Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
                # Sturingsrelatie
                doelAsset_uuid = asset_row.get('Netwerkgegevens_UUID Switch')
                sturingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid,
                                                                 relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing')



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid
                df.at[idx, "sturingsrelatie_uuid"] = sturingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Cameras',
                        columns=["Camera_Object assetId.identificator", "asset_uuid", "voedingsrelatie_uuid",
                                 "bevestigingsrelatie_uuid", "sturingsrelatie_uuid"], index=False, freeze_panes=[1, 1])
        logging.info('Cameras aangemaakt')

    def process_RSS_borden(self, df: pd.DataFrame):
        logging.info('Aanmaken RSS-borden (Rij Strook Signalisatie)')
        logging.info(
            'RSS-borden en (R)VMS-borden. Eerst de groep aanmaken en nadien de Legacy-assets in deze groep plaatsen.')
        logging.info('Seinbrug > RSSGroep > RSS-bord')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("DVM-Bord_UUID Object")
            asset_row_typeURI = asset_row.get("DVM-Bord_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("DVM-Bord_Object assetId.identificator")

            parent_asset_uuid = asset_row.get("Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset")
            if not pd.isna(parent_asset_uuid):
                parent_asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=parent_asset_uuid), None)
            else:
                parent_asset = None

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')
            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                # RSSGroep
                rss_groep_naam = self.construct_rss_groep_naam(rss_naam=asset_row_name)
                rss_groep_asset = self.create_asset_if_missing(
                    typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RSSGroep', asset_naam=rss_groep_naam,
                    parent_uuid=parent_asset.uuid, parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)

                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=rss_groep_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='merk', eigenschapwaarde_nieuw=asset_row.get("DVM-Bord_merk"))



                # Relaties
                # HoortBijrelatie
                doelAsset_uuid = rss_groep_asset.uuid
                hoortbijrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                      doelAsset_uuid=doelAsset_uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')

                # Bevestigingsrelatie
                doelAsset_uuid = asset_row.get('Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
                # Voedingsrelatie
                bronAsset_uuid = asset_row.get('Voedingsrelaties_UUID Voedingsrelatie bronAsset')
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')
                # Sturingsrelatie
                doelAsset_uuid = asset_row.get('Netwerkgegevens_UUID Switch')
                sturingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid,
                                                                 relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing')




                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "hoortbijrelatie_uuid"] = hoortbijrelatie_uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid
                df.at[idx, "sturingsrelatie_uuid"] = sturingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='RSS-borden',
                        columns=["DVM-Bord_Object assetId.identificator", "asset_uuid", "hoortbijrelatie_uuid",
                                 "bevestigingsrelatie_uuid", "voedingsrelatie_uuid", "sturingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('RSS-borden (Rij Strook Signalisatie) aangemaakt')

    def process_RVMS_borden(self, df: pd.DataFrame):
        logging.info('Aanmaken RVMS-borden (Road-side Variable Message Signs)')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("DVM-Bord_UUID Object")
            asset_row_typeURI = asset_row.get("DVM-Bord_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("DVM-Bord_Object assetId.identificator")

            parent_asset_uuid = asset_row.get("Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset")
            if not pd.isna(parent_asset_uuid):
                parent_asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=parent_asset_uuid), None)
            else:
                parent_asset = None

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')
            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                # RVMSGroep
                rvms_groep_naam = self.construct_rvms_groep_naam(rvms_naam=asset_row_name)
                rvms_groep_asset = self.create_asset_if_missing(
                    typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RVMSGroep', asset_naam=rvms_groep_naam,
                    parent_uuid=parent_asset.uuid, parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)

                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=rvms_groep_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.ASSET)


                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='merk', eigenschapwaarde_nieuw=asset_row.get("DVM-Bord_merk"))



                # Relaties
                # HoortBijrelatie
                doelAsset_uuid = rvms_groep_asset.uuid
                hoortbijrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                      doelAsset_uuid=doelAsset_uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')

                # Bevestigingsrelatie
                doelAsset_uuid = asset_row.get('Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
                bevestigingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid,
                                                                          doelAsset_uuid=doelAsset_uuid,
                                                                          relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
                # Voedingsrelatie
                bronAsset_uuid = asset_row.get('Voedingsrelaties_UUID Voedingsrelatie bronAsset')
                voedingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=bronAsset_uuid,
                                                                      doelAsset_uuid=asset.uuid,
                                                                      relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt')
                # Sturingsrelatie
                # nog te activeren: is de sturingsrelatie naar een Switch juist?
                # doelAsset_uuid = asset_row.get('Netwerkgegevens_UUID Switch')
                # sturingsrelatie_uuid = self.create_relatie_if_missing(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid,
                #                                                  relatie_naam='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing')
                sturingsrelatie_uuid = None



                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid
                df.at[idx, "hoortbijrelatie_uuid"] = hoortbijrelatie_uuid
                df.at[idx, "bevestigingsrelatie_uuid"] = bevestigingsrelatie_uuid
                df.at[idx, "voedingsrelatie_uuid"] = voedingsrelatie_uuid
                df.at[idx, "sturingsrelatie_uuid"] = sturingsrelatie_uuid

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='(R)VMS-borden',
                        columns=["DVM-Bord_Object assetId.identificator", "asset_uuid", "hoortbijrelatie_uuid",
                                 "bevestigingsrelatie_uuid", "voedingsrelatie_uuid", "sturingsrelatie_uuid"],
                        index=False, freeze_panes=[1, 1])
        logging.info('RVMS-borden (Road-side Variable Message Signs) aangemaakt')

    def process_portieken_seinbruggen(self, df: pd.DataFrame):
        # Aanmaken van de Portieken-Seinbruggen
        logging.info('Aanmaken van de Portieken / Seinbruggen')
        for idx, asset_row in df.iterrows():
            asset_row_uuid = asset_row.get("Seinbrug_UUID Object")
            asset_row_typeURI = asset_row.get("Seinbrug_Object typeURI")
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get("Seinbrug_Object assetId.identificator")

            asset_row_parent_name = self.construct_installatie_naam(naam=asset_row_name, asset_type=AssetType.SEINBRUGDVM)
            # todo pas de zoekopdracht naar de parent aan naar ofwel een Beheerobject, ofwel een Asset
            # parent_asset = next(self.eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
            parent_asset = next(self.eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True,
                                                                          operator=OperatorEnum.EQ), None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset is None:
                logging.critical('Parent asset is ongekend.')
            else:
                wkt_geometry = self.parse_wkt_geometry(asset_row=asset_row)
                asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                     parent_uuid=parent_asset.uuid, wkt_geometry=wkt_geometry,
                                                     parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

                if asset is None:
                    logging.critical('Asset werd niet aangemaakt')

                # Update eigenschappen
                self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='vrije hoogte', eigenschapwaarde_nieuw=asset_row.get("Seinbrug_vrijeHoogte"))
                # self.update_eigenschap(assetId=asset.uuid, eigenschapnaam_bestaand='', eigenschapwaarde_nieuw=asset_row.get("Seinbrug_RWS/Standaardportiek/tijdelijke seinbrug"))

                # Lijst aanvullen met de naam en diens overeenkomstig uuid
                df.at[idx, "asset_uuid"] = asset.uuid

        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='Portieken-Seinbruggen',
                        columns=["Seinbrug_Object assetId.identificator", "asset_uuid"], index=False,
                        freeze_panes=[1, 1])
        logging.info('Portieken / Seinbruggen aangemaakt')

    def import_data_as_dataframe(self, filepath: Path, sheet_name: str = None):
        """Import data as a dataframe

        Read input data Componententlijst into a DataFrame. Validate the data structure.
        """
        # Read the Excel file
        sheet_df = pd.read_excel(
            filepath,
            header=[0, 1],  # skip the first row and set the second row as headers
            sheet_name=sheet_name
        )
        # Combine multi-level columns into a single string. Concatenate row1 and row2 into one column
        sheet_df.columns = [f'{col[0]}_{col[1]}' for col in sheet_df.columns]

        # drop the first row of the dataframe "in te vullen door: ... and the first columns of the dataframe
        sheet_df = sheet_df.drop(index=sheet_df.index[0], columns=sheet_df.columns[0])

        sheet_df.drop(columns=[col for col in sheet_df.columns if 'Comments' in col], inplace=True)

        # convert NaN to None
        sheet_df = sheet_df.where(pd.notna(sheet_df), None)

        validation_results = self.validate_dataframe_columns(
            df=sheet_df
            , schema_path=Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_validatie.json'
            , schema_key=sheet_name)

        if any(validation_results.values()):
            logging.critical("Validation errors found:")
            for k, v in validation_results.items():
                if v:
                    logging.error(f"{k}: {v}")

            # Raise if errors exist
            raise ValueError("Validation of DataFrame structure failed. See logs for details.")
        else:
            logging.info(f"All validation checks passed for sheet: {sheet_name}")

        return sheet_df

    def validate_dataframe_columns(self, df: pd.DataFrame, schema_path: Path, schema_key: str) -> tuple[
        list[str], list[str]]:
        """
        Validate that the columns of a DataFrame match the expected columns from a JSON file.

        Parameters:
            df (pd.DataFrame): The DataFrame to validate.
            schema_path (str): Path to the JSON file containing expected column definitions.
            schema_key (str): The key inside the JSON under which the expected columns are listed.

        Returns:
            Tuple[List[str], List[str]]: A tuple of (missing_columns, extra_columns)
        """
        # Load expected columns from the JSON file
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)[schema_key]

        expected_columns = [col['name'] for col in schema]
        actual_columns = df.columns.tolist()

        missing_columns = [col for col in expected_columns if col not in actual_columns]
        extra_columns = [col for col in actual_columns if col not in expected_columns]

        type_errors = []
        nullability_errors = []

        for col_def in schema:
            col_name = col_def['name']
            expected_type = col_def.get('type')
            nullable = col_def.get('nullable', True)

            if col_name in df.columns:
                actual_type = str(df[col_name].dtype)
                if expected_type and actual_type != expected_type:
                    type_errors.append(f"Column '{col_name}' expected type '{expected_type}', got '{actual_type}'")

                if not nullable and df[col_name].isnull().any():
                    nullability_errors.append(f"Column '{col_name}' should not contain nulls")

        return {
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "type_errors": type_errors,
            "nullability_errors": nullability_errors
        }

    def create_installatie_if_missing(self, naam: str) -> str:
        """
        Maak de installatie (beheerobject) aan indien onbestaande en geef de uuid terug.

        :param naam: naam van de installatie
        :return: uuid van de installatie
        """
        installatie = next(self.eminfra_client.search_beheerobjecten(naam=naam, actief=True, operator=OperatorEnum.EQ),
                           None)
        if installatie is None:
            logging.info(f'Installatie "{naam}" bestaat nog niet, wordt aangemaakt')
            response_beheerobject = self.eminfra_client.create_beheerobject(naam=naam)
            asset_row_installatie_uuid = response_beheerobject.get("uuid")
        else:
            asset_row_installatie_uuid = installatie.uuid
        logging.info(f'Installatie uuid: {asset_row_installatie_uuid}')
        return asset_row_installatie_uuid

    def create_asset_if_missing(self, typeURI: str, asset_naam: str, parent_uuid: str, wkt_geometry: str | None = None,
                                parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT) -> AssetDTO | None:
        """
        Maak de asset aan indien nog onbestaande en geef de asset terug
        Update de toestand van de asset
        Update locatie (indien aanwezig)
        Add bestekkoppeling (if missing)

        :param typeURI: asset typeURI
        :asset_naam: asset naam
        :parent_uuid: parent uuid
        :parent_asset_type:
        :return: asset
        """
        asset = None
        assettype_uuid = self.assettype_mapping_dict.get(typeURI, None)
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(expressions=[
                ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype_uuid}')]),
                ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=f'{asset_naam}')],
                              logicalOp=LogicalOpEnum.AND)
            ]))
        assets_list = list(self.eminfra_client.search_assets(query_dto=query_dto))

        nbr_assets = len(assets_list)
        if nbr_assets > 1:
            logging.critical(
                f'Er bestaan meerdere assets (#{nbr_assets}) van het type: {typeURI}, met naam: {asset_naam}')
        elif nbr_assets == 1:
            logging.debug(f'Asset {asset_naam} ({typeURI}) bestaat al.')
            asset = assets_list[0]
        elif nbr_assets == 0:
            logging.debug(f'Asset {asset_naam} ({typeURI}) bestaat nog niet en wordt aangemaakt.')
            asset_dict = self.eminfra_client.create_asset(
                parent_uuid=parent_uuid,
                naam=asset_naam,
                typeUuid=assettype_uuid,
                parent_asset_type=parent_asset_type)
            asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=asset_dict.get('uuid')), None)

        else:
            logging.critical('Unknown error')
            raise ValueError(
                f'Could not create new asset. typeURI: {typeURI}, asset_naam: {asset_naam}, parent_uuid: {parent_uuid}')

        # Update toestand
        self.update_toestand(asset=asset)

        # Update eigenschap locatie
        if wkt_geometry:
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{wkt_geometry}"')
            self.eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid,
                                                                     wkt_geom=wkt_geometry)

        # Bestekkoppelingen
        self.add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=self.eDelta_dossiernummer,
                                            start_datetime=self.start_datetime)
        return asset

    def create_relatie_if_missing(self, bronAsset_uuid: str, doelAsset_uuid: str, relatie_naam: str) -> str | None:
        """
        Maak een relatie aan tussen 2 assets indien deze nog niet bestaat.
        Geeft de relatie-uuid weeer.

        :param bronAsset_uuid:
        :param doelAsset_uuid:
        :param relatie_naam:
        :return:
        """
        if pd.isna(bronAsset_uuid) or pd.isna(doelAsset_uuid):
            logging.debug(f'doelAsset_uuid ({doelAsset_uuid}) of bronAsset_uuid ({bronAsset_uuid}) zijn None. Relatie wordt niet aangemaakt.')
            return None

        if response := self.eminfra_client.search_assetrelaties_OTL(
                bronAsset_uuid=bronAsset_uuid, doelAsset_uuid=doelAsset_uuid
        ):
            logging.debug(
                f'{relatie_naam}-relatie tussen {bronAsset_uuid} en {doelAsset_uuid} bestaat al. Returns relatie-uuid')
            return (
                response[0]
                .get("RelatieObject.assetId")
                .get("DtcIdentificator.identificator")[:36]
            )
        else:
            logging.debug(
                f'{relatie_naam}-relatie tussen {bronAsset_uuid} en {doelAsset_uuid} wordt aangemaakt. Returns relatie-uuid')
            kenmerkType_id, relatieType_id = self.eminfra_client.get_kenmerktype_and_relatietype_id(relatie_uri=relatie_naam)

            relatie_uuid = self.eminfra_client.create_assetrelatie(relatieType_uuid=relatieType_id, bronAsset_uuid=bronAsset_uuid, doelAsset_uuid=doelAsset_uuid)
            return relatie_uuid
            # self.eminfra_client.add_relatie(assetId=bronAsset_uuid, kenmerkTypeId=kenmerkType_id, relatieTypeId=relatieType_id, doel_assetId=doelAsset_uuid)
            # relatie = next(self.eminfra_client.search_relaties(assetId=bronAsset_uuid, kenmerkTypeId=kenmerkType_id, relatieTypeId=relatieType_id), None)
            # if relatie:
            #     return relatie.uuid
            # else:
            #     return None

    def _construct_installatie_naam_kast(self, naam: str) -> str:
        """
        Verwijder suffix ".K".
        Hernoem letter P/N/M door X. Deze letter duidt de rijrichting aan (Positief, Negatief, Middenberm) en volgt net na de naam van de rijweg.
        Voorbeeld:
        kastnaam: A13M0.5.K
        installatie_naam: A13X0.5

        :param naam:
        :return:
        """
        # Step 1: Remove suffix ".K" if present
        if naam.endswith('.K'):
            temp_installatie_naam = naam[:-2]
        else:
            raise ValueError(f"Kastnaam {naam} eindigt niet op '.K'")

        if match := re.search(r'(.*)([MPN])(?!.*[MPN])', temp_installatie_naam):
            installatie_naam = match[1] + 'X' + temp_installatie_naam[match.end():]
        else:
            raise ValueError("De syntax van de kast bevat geen letter 'P', 'N' of 'M'.")
        return installatie_naam

    def _construct_installatie_naam_hscabine(self, naam: str) -> str:
        if naam.endswith('.HSCabine'):
            installatie_naam = naam[:-9]
        else:
            raise ValueError(f"De naam van de HSCabine ({naam}) eindigt niet op '.HSCabine'")
        return installatie_naam

    def _construct_installatie_naam_hoogspanningsdeel(self, naam: str) -> str:
        if naam.endswith('.HSDeel'):
            installatie_naam = naam.replace('.HSDeel', '.HSCabine')
        else:
            raise ValueError(f"De naam van het Hoogspanningsdeel ({naam}) eindigt niet op '.HSDeel'")
        return installatie_naam

    def _construct_installatie_naam_laagspanningsdeel(self, naam: str) -> str:
        if naam.endswith('.LSDeel'):
            installatie_naam = naam.replace('.LSDeel', '.HSCabine')
        else:
            raise ValueError(f"De naam van het Laagspanningsdeel ({naam}) eindigt niet op '.LSDeel'")
        return installatie_naam

    def _construct_installatie_naam_hoogspanning(self, naam: str) -> str:
        if naam.endswith('.HS'):
            installatie_naam = naam.replace('.HS', '.HSCabine')
        else:
            raise ValueError(f"De naam van de Hoogspanning ({naam}) eindigt niet op '.HS'")
        return installatie_naam

    def _construct_installatie_naam_segmentcontroller(self, naam: str) -> str:
        if re.search(pattern='.*\.SC\d', string=naam):
            installatie_naam = naam[:-4]
        else:
            raise ValueError(f"De naam van de Segmentcontroller ({naam}) eindigt niet op '.SC1'")
        return installatie_naam

    def _construct_installatie_naam_wegverlichtingsgroep(self, naam: str) -> str:
        if naam.endswith('.WV'):
            installatie_naam = naam[:-3]
        else:
            raise ValueError(f"De naam van de Wegverlichtingsgroep ({naam}) eindigt niet op '.WV'")
        return installatie_naam

    def _construct_installatie_naam_switch(self, naam: str) -> str:
        if re.search(pattern='.*-A.{1}\d', string=naam):
            installatie_naam = naam[:-4]
        else:
            raise ValueError(f"De naam van de Switch ({naam}) eindigt niet op '-A?1'")
        return installatie_naam

    def _construct_installatie_naam_teletransmissieverbinding(self, naam: str) -> str:
        if re.search(pattern='.*\.ODF', string=naam):
            installatie_naam = naam[:-4]
        else:
            raise ValueError(f"De naam van de Teletransmissieverbinding ({naam}) eindigt niet op '.ODF'")
        return installatie_naam

    def _construct_installatie_naam_wvlichtmast(self, naam: str) -> str:
        if re.search(pattern='^.+\..*', string=naam):
            installatie_naam = naam.split('.', 1)[0]
        else:
            raise ValueError(f"De naam van WVLichtmast ({naam}) voldoet niet aan de syntax regels.")
        return installatie_naam

    def _construct_installatie_naam_seinbrug(self, naam: str) -> str:
        if naam.endswith('.S'):
            installatie_naam = naam[:-2]
        else:
            raise ValueError(f"De naam van de Seinbrug ({naam}) eindigt niet op '.S'")
        return installatie_naam

    def construct_installatie_naam(self, naam: str, asset_type: AssetType) -> str:
        # kastnaam: str = None, hscabinenaam: str = None, hoogspanningsdeelnaam: str = None, laagspanningsdeelnaam: str = None, hoogspanningnaam: str = None, segmentcontrollernaam: str = None) -> str:
        """
        Bouw de installatie naam op basis van het asset-type
        """
        installatie_naam = ''
        if asset_type.name == 'WEGKANTKAST':
            installatie_naam = self._construct_installatie_naam_kast(naam=naam)
        elif asset_type.name == 'HSCABINE':
            installatie_naam = self._construct_installatie_naam_hscabine(naam=naam)
        elif asset_type.name == 'HSDEEL':
            installatie_naam = self._construct_installatie_naam_hoogspanningsdeel(naam=naam)
        elif asset_type.name == 'LSDEEL':
            installatie_naam = self._construct_installatie_naam_laagspanningsdeel(naam=naam)
        elif asset_type.name == 'HS':
            installatie_naam = self._construct_installatie_naam_hoogspanning(naam=naam)
        elif asset_type.name == 'SEGC':
            installatie_naam = self._construct_installatie_naam_segmentcontroller(naam=naam)
        elif asset_type.name == 'WVGROEP':
            installatie_naam = self._construct_installatie_naam_wegverlichtingsgroep(naam=naam)
        elif asset_type.name == 'IP':
            installatie_naam = self._construct_installatie_naam_switch(naam=naam)
        elif asset_type.name == 'TT':
            installatie_naam = self._construct_installatie_naam_teletransmissieverbinding(naam=naam)
        elif asset_type.name == 'WVLICHTMAST':
            installatie_naam = self._construct_installatie_naam_wvlichtmast(naam=naam)
        elif asset_type.name == 'SEINBRUGDVM':
            installatie_naam = self._construct_installatie_naam_seinbrug(naam=naam)
        return installatie_naam

    def validate_asset(self, uuid: str = None, naam: str = None, stop_on_error: bool = True) -> None:
        """
        Controleer het bestaan van een asset op basis van diens uuid.
        Valideer nadien of de nieuwe naam overeenstemt met de naam van de bestaande asset.

        :param uuid: asset uuid
        :param naam: asset name
        :param stop_on_error: Raise Error (default True)
        :type stop_on_error: boolean
        :return: None
        """
        logging.debug('Valideer of een asset reeds bestaat en of diens naam overeenkomt.')
        asset = next(self.eminfra_client.search_asset_by_uuid(uuid), None)

        if asset is None:
            logging.error(f'Asset {uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')
            if stop_on_error:
                raise ValueError(f'Asset {uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')

        if str(naam) != str(asset.naam):  # don't remove the casting
            logging.error(
                f'Asset {uuid} naam {naam} komt niet overeen met de bestaande naam {asset.naam}.')
            if stop_on_error:
                raise ValueError(
                    f'Asset {uuid} naam {naam} komt niet overeen met de bestaande naam {asset.naam}.')
        return None

    def parse_wkt_geometry(self, asset_row) -> str:
        matching_column_x = [col for col in asset_row.index if 'Positie X (Lambert 72)' in col]
        asset_row_x = asset_row[matching_column_x[0]] if matching_column_x else None
        matching_column_y = [col for col in asset_row.index if 'Positie Y (Lambert 72)' in col]
        asset_row_y = asset_row[matching_column_y[0]] if matching_column_y else None
        matching_column_z = [col for col in asset_row.index if 'Positie Z (Lambert 72, optioneel)' in col]
        asset_row_z = asset_row[matching_column_z[0]] if matching_column_z else None
        if pd.isna(asset_row_z):
            asset_row_z = 0
        if pd.isna(asset_row_x) or pd.isna(asset_row_y):
            return None
        return f'POINT Z ({asset_row_x} {asset_row_y} {asset_row_z})'

    def add_bestekkoppeling_if_missing(self, asset_uuid: str, eDelta_dossiernummer: str,
                                       start_datetime: datetime) -> None:
        """
        Voeg een specifieke bestekkoppeling toe, indien die nog niet bestaat bij een bepaalde asset.

        :param asset_uuid:
        :param eDelta_dossiernummer:
        :param start_datetime:
        :return:
        """
        # check if the eDelta_dossiernummer is valid.
        bestekref = self.eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
        if bestekref is None:
            logging.critical(
                f'Bestek met eDelta_dossiernumer {eDelta_dossiernummer} werd niet teruggevonden. Omgeving: {self.environment.name}')

        huidige_bestekkoppelingen = self.eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
        # check if there are currently no bestekkkoppelingen.
        if all(
                bestekkoppeling.bestekRef.eDeltaDossiernummer
                != eDelta_dossiernummer
                for bestekkoppeling in huidige_bestekkoppelingen
        ):
            self.eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                                    start_datetime=start_datetime)

    def append_columns(self, df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
        """
        Append new columns to the dataframe with default value None.
        :param df: Dataframe
        :param columns: New columns
        :return: Dataframe
        """
        if columns is None:
            columns = ["asset_uuid"]
        # append new columns
        for col in columns:
            df[col] = None
        return df

    def build_query_MIVLVE(self, naam: str) -> QueryDTO:
        return QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                        expansions=ExpansionsDTO(fields=['parent']),
                        selection=SelectionDTO(
                            expressions=[ExpressionDTO(
                                terms=[
                                    TermDTO(property='actief', operator=OperatorEnum.EQ, value=True),
                                    TermDTO(property='naam', operator=OperatorEnum.EQ, value=naam,
                                            logicalOp=LogicalOpEnum.AND),
                                    TermDTO(property='type', operator=OperatorEnum.EQ, value=self.assettype_mapping_dict.get('https://lgc.data.wegenenverkeer.be/ns/installatie#Kast'),
                                            logicalOp=LogicalOpEnum.AND)
                                ])]))

    def build_query_MIVMeetpunten(self, naam: str) -> QueryDTO:
        return QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                        expansions=ExpansionsDTO(fields=['parent']),
                        selection=SelectionDTO(
                            expressions=[ExpressionDTO(
                                terms=[
                                    TermDTO(property='naam', operator=OperatorEnum.EQ, value=naam),
                                    TermDTO(property='type', operator=OperatorEnum.EQ, value=self.assettype_mapping_dict.get('https://lgc.data.wegenenverkeer.be/ns/installatie#MIVLVE'),
                                            logicalOp=LogicalOpEnum.AND)
                                ])]))

    def construct_rss_groep_naam(self, rss_naam: str) -> str:
        if re.search(pattern='^.+\..*', string=rss_naam):
            rss_groep_naam = rss_naam.split('.', 1)[0]
        else:
            raise ValueError(f"De naam van de RSS ({rss_naam}) voldoet niet aan de syntax regels.")
        return rss_groep_naam

    def construct_rvms_groep_naam(self, rvms_naam: str) -> str:
        if re.search(pattern='^.+\..*', string=rvms_naam):
            rvms_groep_naam = rvms_naam.split('.', 1)[0]
        else:
            raise ValueError(f"De naam van de RVMS ({rvms_naam}) voldoet niet aan de syntax regels.")
        return rvms_groep_naam

    def update_eigenschap(self, assetId:str, eigenschapnaam_bestaand: str, eigenschapwaarde_nieuw: str) -> None:
        # Get all eigenschappen from asset
        eigenschappen = self.eminfra_client.get_eigenschappen(assetId=assetId)

        eigenschap_bestaand = [e for e in eigenschappen if e.eigenschap.naam == eigenschapnaam_bestaand]

        if eigenschap_bestaand:
            eigenschap_bestaand = eigenschap_bestaand[0]
            logging.info(f'Eigenschap "{eigenschap_bestaand.eigenschap.naam}" bestaat')
            if eigenschap_bestaand.typedValue.get('value', None) == eigenschapwaarde_nieuw:
                logging.info(
                    f'Eigenschap "{eigenschap_bestaand.eigenschap.naam}" waarde is identiek aan de nieuwe waarde "{eigenschapwaarde_nieuw}": geen update')
            else:
                logging.info(
                    f'Eigenschap "{eigenschap_bestaand}" waarde wordt overschreven door een nieuwe waarde "{eigenschapwaarde_nieuw}": update')
                eigenschap_bestaand.typedValue.update({"value": eigenschapwaarde_nieuw})
                self.eminfra_client.update_eigenschap(assetId=assetId, eigenschap=eigenschap_bestaand)

    def update_toestand(self, asset: AssetDTO) -> None:
        """
        Update de toestand van een asset.

        Doe niets indien de toestand van de asset 'IN_GEBRUIK' of 'OVERGEDRAGEN' is.
        Wijzig de toestand naar 'IN_OPBOUW' in alle andere situaties.

        :param asset:
        :return:
        """
        toestand = asset.toestand.value
        if toestand in [AssetDTOToestand.IN_GEBRUIK.value, AssetDTOToestand.OVERGEDRAGEN.value]:
            logging.info(f'Asset {asset.uuid} heeft toestand "{toestand}" en wordt niet geüpdatet')
        elif asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW.value}"')
            self.eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)


if __name__ == '__main__':
    bypass = BypassProcessor(
        environment=Environment.TEI
        , input_path_componentenlijst=Path(
            __file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250507_TEI.xlsx'
        , output_excel_path=Path(
            __file__).resolve().parent / 'data' / 'output' / f'lantis_bypass_{datetime.now().strftime(format="%Y-%m-%d")}.xlsx'
    )

    bypass.import_data()

    # todo de code verder in functies wrappen om de leesbaarheid te verbeteren.
    logging.info('Aanmaken Boomstructuur voor installaties onder Wegkantkast')
    logging.info('Aanmaken installaties')
    bypass.process_installatie(df=bypass.df_assets_wegkantkasten, column_name='Wegkantkast_Object assetId.identificator', asset_type=AssetType.WEGKANTKAST)

    logging.info('Aanmaken Wegkantkasten')
    asset_info = AssetInfo(asset_type=AssetType.WEGKANTKAST, column_typeURI='Wegkantkast_Object typeURI', column_uuid='Wegkantkast_UUID Object', column_name='Wegkantkast_Object assetId.identificator')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)
    bypass.process_assets(df=bypass.df_assets_wegkantkasten, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken LSDeel')
    asset_info = AssetInfo(asset_type=AssetType.LSDEEL, column_uuid='Laagspanningsdeel_UUID LSDeel', column_name='Laagspanningsdeel_Naam LSDeel', column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Wegkantkast_UUID Object', column_parent_name='Wegkantkast_Object assetId.identificator')
    bevestigingsrelatie = RelatieInfo(bronAsset_uuid=None
                                      , doelAsset_uuid='Wegkantkast_UUID Object'
                                      , uri=RelatieType.BEVESTIGING)
    voedingsrelatie = RelatieInfo(bronAsset_uuid='Voedingsrelatie (oorsprong)_UUID Voedingsrelatie bronAsset'
                                  , doelAsset_uuid='Laagspanningsdeel_UUID LSDeel'
                                  , uri=RelatieType.VOEDT)
    bypass.process_assets(df=bypass.df_assets_wegkantkasten
                          , asset_info=asset_info
                          , parent_asset_info=parent_asset_info
                          , relatie_infos=[bevestigingsrelatie, voedingsrelatie])

    logging.info('Aanmaken IP')
    asset_info = AssetInfo(asset_type=AssetType.IP, column_uuid='Switch gegevens_UUID Switch', column_name='Switch gegevens_Object assetId.identificator', column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#IP')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Wegkantkast_UUID Object')
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Wegkantkast_UUID Object')
    bypass.process_assets(df=bypass.df_assets_wegkantkasten, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[bevestigingsrelatie])

    logging.info('Aanmaken Teletransmissieverbinding (TT)')
    asset_info = AssetInfo(asset_type=AssetType.TT, column_uuid='Switch gegevens_UUID Teletransmissieverbinding', column_name='Switch gegevens_Naam Teletransmissieverbinding', column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#TT')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Wegkantkast_UUID Object')
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Wegkantkast_UUID Object')
    bypass.process_assets(df=bypass.df_assets_wegkantkasten, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[bevestigingsrelatie])

    logging.info('Aanmaken Meetlussen MIVLVE')
    asset_info = AssetInfo(asset_type=AssetType.MIVLVE, column_uuid='LVE_UUID Object', column_name='LVE_Object assetId.identificator', column_typeURI='LVE_Object typeURI')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset', column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator')
    voedingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt',
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset', doelAsset_uuid=None)
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    bypass.process_assets(df=bypass.df_assets_mivlve, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[voedingsrelatie, bevestigingsrelatie])
    # bypass.process_mivlve(df=bypass.df_assets_mivlve)

    logging.info('Aanmaken Switch')
    asset_info = AssetInfo(asset_type=AssetType.IP, column_uuid='Netwerkgegevens_UUID Switch', column_name='Netwerkgegevens_Switch assetId.identificator', column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#IP')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='LVE_UUID Object', column_parent_name='LVE_Object assetId.identificator')
    bypass.process_assets(df=bypass.df_assets_mivlve, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken Poort')
    asset_info = AssetInfo(asset_type=AssetType.POORT, column_uuid='Netwerkgegevens_UUID Poort', column_name='Netwerkgegevens_assetId.identificator Poort', column_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='LVE_UUID Object', column_parent_name='LVE_Object assetId.identificator')
    sturingrelatie = RelatieInfo(uri=RelatieType.STURING, bronAsset_uuid='Netwerkgegevens_UUID Poort', doelAsset_uuid='LVE_UUID Object')
    bypass.process_assets(df=bypass.df_assets_mivlve, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[sturingrelatie])

    logging.info('Aanmaken Meetpunten')
    asset_info = AssetInfo(asset_type=AssetType.MPT, column_uuid='Meetpunt_UUID Object', column_name='Meetpunt_Object assetId.identificator', column_typeURI='Meetpunt_Object typeURI')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Sturingsrelaties_UUID Sturingsrelatie bronAsset', column_parent_name='Sturingsrelatie_Sturingsrelatie bron AssetId.identificator')
    eigenschap_infos = [
        EigenschapInfo(eminfra_eigenschap_name='aansluiting', column_eigenschap_name='Meetpunt_Aansluiting')
        , EigenschapInfo(eminfra_eigenschap_name='uitslijprichting', column_eigenschap_name='Meetpunt_Uitslijprichting')
        , EigenschapInfo(eminfra_eigenschap_name='wegdek', column_eigenschap_name='Meetpunt_Wegdek')
    ]
    sturingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing',
                                      bronAsset_uuid='Sturingsrelaties_UUID Sturingsrelatie bronAsset', doelAsset_uuid=None)
    bypass.process_assets(df=bypass.df_assets_mivlve, asset_info=asset_info, parent_asset_info=parent_asset_info, eigenschap_infos=eigenschap_infos, relatie_infos=[sturingsrelatie])
    # bypass.process_mivmeetpunten(df=bypass.df_assets_mivmeetpunten)

    logging.info('Aanmaken Seinbrug DVM')
    asset_info = AssetInfo(asset_type=AssetType.SEINBRUGDVM, column_typeURI='Seinbrug_Object typeURI', column_uuid='Seinbrug_UUID Object', column_name='Seinbrug_Object assetId.identificator')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)
    eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='vrije hoogte', column_eigenschap_name='Seinbrug_vrijeHoogte')
    bypass.process_assets(df=bypass.df_assets_portieken_seinbruggen, asset_info=asset_info, parent_asset_info=parent_asset_info, eigenschap_infos=[eigenschap_info])

    logging.info('Aanmaken RSS-Groep')
    asset_info = AssetInfo(asset_type=AssetType.RSSGROEP, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RSSGroep', column_name='HoortBij Relatie voor RSS-groep_HoortBij doelAssetId.identificator', column_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator', column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    bypass.process_assets(df=bypass.df_assets_RSS_borden, asset_info=asset_info, parent_asset_info=parent_asset_info) # geen eigenschappen, noch relaties voor de RSS-groep

    logging.info('Aanmaken RSSBord')
    asset_info = AssetInfo(asset_type=AssetType.RSSBORD, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RSSBord', column_name='DVM-Bord_Object assetId.identificator', column_uuid='DVM-Bord_UUID Object')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_name='HoortBij Relatie voor RSS-groep_HoortBij doelAssetId.identificator', column_parent_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset')
    eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='merk', column_eigenschap_name='DVM-Bord_merk')
    hoortbijrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij', bronAsset_uuid=None, doelAsset_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset')
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    voedingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt', bronAsset_uuid='Voedingsrelaties_Voedingsrelatie bronAssetId.identificator', doelAsset_uuid=None)
    bypass.process_assets(df=bypass.df_assets_RSS_borden, asset_info=asset_info, parent_asset_info=parent_asset_info, eigenschap_infos=[eigenschap_info], relatie_infos=[hoortbijrelatie, bevestigingsrelatie, voedingsrelatie])

    # todo hier de Netwerkgegevens aanvullen

    logging.info('Aanmaken RVMS-Groep')
    asset_info = AssetInfo(asset_type=AssetType.RVMSGROEP, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RVMSGroep', column_name='HoortBij Relatie_HoortBij doelAssetId.identificator', column_uuid='HoortBij Relatie_UUID HoortBij doelAsset')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator', column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    bypass.process_assets(df=bypass.df_assets_RVMS_borden, asset_info=asset_info, parent_asset_info=parent_asset_info) # geen eigenschappen, noch relaties voor de RSS-groep

    logging.info('Aanmaken RVMS-Bord')
    asset_info = AssetInfo(asset_type=AssetType.RSSBORD, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#RVMSBord', column_name='DVM-Bord_Object assetId.identificator', column_uuid='DVM-Bord_UUID Object')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_name='HoortBij Relatie_HoortBij doelAssetId.identificator', column_parent_uuid='HoortBij Relatie_UUID HoortBij doelAsset')
    eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='merk', column_eigenschap_name='DVM-Bord_merk')
    hoortbijrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij', bronAsset_uuid=None, doelAsset_uuid='HoortBij Relatie_UUID HoortBij doelAsset')
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    voedingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt', bronAsset_uuid='Voedingsrelaties_Voedingsrelatie bronAssetId.identificator', doelAsset_uuid=None)
    bypass.process_assets(df=bypass.df_assets_RVMS_borden, asset_info=asset_info, parent_asset_info=parent_asset_info, eigenschap_infos=[eigenschap_info], relatie_infos=[hoortbijrelatie, bevestigingsrelatie, voedingsrelatie])

    logging.info('Aanmaken Galgpaal')
    asset_info = AssetInfo(asset_type=AssetType.GALGPAAL, column_typeURI='Galgpaal_Object typeURI', column_uuid='Galgpaal_UUID Object', column_name='Galgpaal_Object assetId.identificator')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)
    bypass.process_assets(df=bypass.df_assets_galgpaal, asset_info=asset_info, parent_asset_info=parent_asset_info)


    logging.info('Aanmaken Camera-Groep')
    logging.debug('1 cameragroep per camera aanmaken')
    asset_info = AssetInfo(asset_type=AssetType.CAMGROEP, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#CamGroep', column_name='Camera_Object assetId.identificator', column_uuid='Camera_UUID Object')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT, column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator', column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    bypass.process_assets(df=bypass.df_assets_cameras, asset_info=asset_info, parent_asset_info=parent_asset_info) # geen eigenschappen, noch relaties voor de RSS-groep

    logging.info('Aanmaken Camera')
    logging.debug('Camgroep onder 1 installatie plaatsen')
    asset_info = AssetInfo(asset_type=AssetType.CAMERA, column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#Camera', column_name='Camera_Object assetId.identificator', column_uuid='Camera_UUID Object')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator', column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    eigenschap_1 = EigenschapInfo(eminfra_eigenschap_name='isPtz', column_eigenschap_name='isPtz')
    eigenschap_2 = EigenschapInfo(eminfra_eigenschap_name='heeftAid', column_eigenschap_name='heeftAid')
    voedingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt', bronAsset_uuid='Voedingsrelaties_Voedingsrelatie bronAssetId.identificator', doelAsset_uuid=None)
    bevestigingsrelatie = RelatieInfo(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', bronAsset_uuid=None, doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
    bypass.process_assets(df=bypass.df_assets_cameras, asset_info=asset_info, parent_asset_info=parent_asset_info, eigenschap_infos=[eigenschap_1, eigenschap_2], relatie_infos=[voedingsrelatie, bevestigingsrelatie])

    logging.info('Aanmaken Switch')
    asset_info = AssetInfo(asset_type=AssetType.SWITCH, column_uuid='Netwerkgegevens_UUID Switch', column_name='Netwerkgegevens_Switch assetId.identificator', column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#Switch')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Camera_UUID Object', column_parent_name='Camera_Object assetId.identificator')
    bypass.process_assets(df=bypass.df_assets_cameras, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken Poort')
    asset_info = AssetInfo(asset_type=AssetType.POORT, column_uuid='Netwerkgegevens_UUID Poort', column_name='Netwerkgegevens_assetId.identificator Poort', column_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='Camera_UUID Object', column_parent_name='Camera_Object assetId.identificator')
    sturingrelatie = RelatieInfo(uri=RelatieType.STURING, bronAsset_uuid='Netwerkgegevens_UUID Poort', doelAsset_uuid='Camera_UUID Object')
    bypass.process_assets(df=bypass.df_assets_cameras, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[sturingrelatie])


    # Boomstructuur van de Hoogspanningscabine
    logging.info('Aanmaken Boomstructuur voor installaties onder Wegkantkast')
    logging.info('Aanmaken installaties')
    bypass.process_installatie(df=bypass.df_assets_voeding, column_name='HSCabine_Object assetId.identificator', asset_type=AssetType.HSCABINE)
    logging.info('Aanmaken Hoogspannings Cabine')

    logging.info('Aanmaken HSCabine')
    asset_info = AssetInfo(asset_type=AssetType.HSCABINE, column_name='HSCabine_Object assetId.identificator', column_uuid='HSCabine_UUID Object', column_typeURI='HSCabine_Object typeURI')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT, column_parent_uuid=None, column_parent_name=None)
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken HSDeel')
    asset_info = AssetInfo(asset_type=AssetType.HSDEEL, column_name='Hoogspanningsdeel_Naam HSDeel', column_uuid='Hoogspanningsdeel_UUID HSDeel', column_typeURI='Hoogspanningsdeel_HSDeel lgc:installatie')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='HSCabine_UUID Object', column_parent_name='HSCabine_Object assetId.identificator')
    bevestigingsrelatie = RelatieInfo(uri=RelatieType.BEVESTIGING, bronAsset_uuid=None, doelAsset_uuid='HSCabine_UUID Object')
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[bevestigingsrelatie])

    logging.info('Aanmaken LSDeel')
    asset_info = AssetInfo(asset_type=AssetType.LSDEEL, column_name='Laagspanningsdeel_Naam LSDeel', column_uuid='Laagspanningsdeel_UUID LSDeel', column_typeURI='Laagspanningsdeel_LSDeel lgc:installatie')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='HSCabine_UUID Object', column_parent_name='HSCabine_Object assetId.identificator')
    bevestigingsrelatie = RelatieInfo(uri=RelatieType.BEVESTIGING, bronAsset_uuid=None, doelAsset_uuid='HSCabine_UUID Object')
    voedingsrelatie = RelatieInfo(uri=RelatieType.VOEDT, bronAsset_uuid='Hoogspanningsdeel_UUID HSDeel', doelAsset_uuid=None)
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[bevestigingsrelatie, voedingsrelatie])

    logging.info('Aanmaken Hoogspanning')
    asset_info = AssetInfo(asset_type=AssetType.HS, column_name='Hoogspanning_Naam HS', column_uuid='Hoogspanning_UUID HS', column_typeURI='Hoogspanning_HS lgc:installatie')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET, column_parent_uuid='HSCabine_UUID Object', column_parent_name='HSCabine_Object assetId.identificator')
    bevestigingsrelatie = RelatieInfo(uri=RelatieType.BEVESTIGING, bronAsset_uuid=None, doelAsset_uuid='HSCabine_UUID Object')
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info, relatie_infos=[bevestigingsrelatie])

    logging.info('Aanmaken DNBHoogspanning')
    asset_info = AssetInfo(asset_type=AssetType.DNBHOOGSPANNING, column_name='DNBHoogspanning_Object assetId.identificator', column_uuid='DNBHoogspanning_UUID Object', column_typeURI='DNBHoogspanning_Object typeURI')
    eigenschappen = [EigenschapInfo(eminfra_eigenschap_name='eanNummer', column_eigenschap_name='DNBHoogspanning_eanNummer')]
    eigenschappen.append(EigenschapInfo(eminfra_eigenschap_name='referentieDNB', column_eigenschap_name='DNBHoogspanning_referentieDNB'))
    hoortbijrelatie = RelatieInfo(uri=RelatieType.HOORTBIJ, bronAsset_uuid=None, doelAsset_uuid='Hoogspanning_UUID HS') # Hoortbij relatie van OTL naar Legacy-asset
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, eigenschap_infos=eigenschappen, relatie_infos=[hoortbijrelatie])

    logging.info('Aanmaken EnergiemeterDNB')
    asset_info = AssetInfo(asset_type=AssetType.ENERGIEMETERDNB, column_name='EnergiemeterDNB_Object assetId.identificator', column_uuid='EnergiemeterDNB_UUID Object', column_typeURI='EnergiemeterDNB_Object typeURI')
    eigenschappen = [EigenschapInfo(eminfra_eigenschap_name='meternummer', column_eigenschap_name='meternummer')]
    hoortbijrelatie = RelatieInfo(uri=RelatieType.HOORTBIJ, bronAsset_uuid=None, doelAsset_uuid='Hoogspanning_UUID HS') # Hoortbij relatie van OTL naar Legacy-asset
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, eigenschap_infos=eigenschappen, relatie_infos=[hoortbijrelatie])

    logging.info('Aanmaken SegmentController')
    asset_info = AssetInfo(asset_type=AssetType.SEGC, column_name='Segmentcontroller_Naam SC', column_uuid='Segmentcontroller_UUID SC', column_typeURI='Segmentcontroller_SC TypeURI')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT, column_parent_uuid=None, column_parent_name=None)
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken Wegverlichtingsgroep')
    asset_info = AssetInfo(asset_type=AssetType.WVGROEP, column_name='Wegverlichtingsgroep_Naam WV', column_uuid='Wegverlichtingsgroep_UUID WV', column_typeURI='Wegverlichtingsgroep_WV lgc:installatie')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT, column_parent_uuid=None, column_parent_name=None)
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken IP')
    asset_info = AssetInfo(asset_type=AssetType.IP, column_name='Switch gegevens_Object assetId.identificator',
                           column_uuid='Switch gegevens_UUID switch',
                           column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#IP')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                        column_parent_uuid=None, column_parent_name=None) # Kan de juiste parent worden afgeleid?
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info)

    logging.info('Aanmaken WVLichtmast')
    asset_info = AssetInfo(asset_type=AssetType.WVLICHTMAST, column_name='WVLichtmast_Object assetId.identificator', column_uuid='WVLichtmast_UUID Object', column_typeURI='WVLichtmast_Object typeURI')
    parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT, column_parent_uuid=None, column_parent_name=None)
    voedingsrelatie = RelatieInfo(uri=RelatieType.VOEDT, bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset', doelAsset_uuid=None)
    bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info)