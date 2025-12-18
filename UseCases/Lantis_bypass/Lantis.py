import json
import logging
from datetime import datetime
import re

from API.EMInfraDomain import OperatorEnum, BoomstructuurAssetTypeEnum, \
    AssetDTOToestand, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, ExpressionDTO, LogicalOpEnum, \
    AssetDTO, EigenschapValueUpdateDTO, RelatieEnum
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

from UseCases.Lantis_bypass.LantisDomain import AssetType, RelatieInfo, ParentAssetInfo, AssetInfo, EigenschapInfo
from UseCases.Lantis_bypass.LantisFunctions import map_relatie, map_status
from UseCases.utils import create_relatie_if_missing


class BypassProcessor:
    def __init__(self, environment: Environment = Environment.TEI,
                 settings_path: Path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources'
                                                       '/settings_SyncOTLDataToLegacy.json'
                 , eDelta_dossiernummer: str = "INTERN-095"
                 , input_path_componentenlijst: Path = Path(
                __file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250507.xlsx'
                 , output_excel_path: Path = Path(
                __file__).resolve().parent / 'data' / 'output' / f'lantis_bypass_{datetime.now().strftime(format="%Y-%m-%d")}.xlsx'
                 , startdatum_bestekkoppeling: datetime = datetime(2024, 9, 1)
                 ):
        """
        Initializes the LantisBypass class with specified parameters.

        Args:
            environment (Environment, optional): The environment to use. Defaults to Environment.TEI.
            settings_path (Path, optional): The path to the settings file. Defaults to the default settings path.
            eDelta_dossiernummer (str, optional): The eDelta dossier number. Defaults to "INTERN-095".
            input_path_componentenlijst (Path, optional): The path to the input component list Excel file. Defaults to the default input path.
            output_excel_path (Path, optional): The path to the output Excel file. Defaults to a file with the current date in the name.
            startdatum_bestekkoppeling (datetime, optional): The start date of the contract link. Defaults to September 1, 2024.

        Returns:
            None
        """
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
                    "Field": ["Author", "Timestamp", "Description", "Environment"],
                    "Value": [
                        'Dries Verdoodt - dries.verdoodt@mow.vlaanderen.be',
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'Lantis Bypass | aanmaken van nieuwe assets in EM-infra',
                        self.environment.value[0]
                    ]
                })
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)

        logging.info(f'Output file path: {self.output_excel_path}')

        self.setup_mapping_dict_typeURI()
        self.setup_mapping_dict_eigenschappen()

    def setup_mapping_dict_typeURI(self):
        """
        Sets up the mapping dictionary for typeURI based on the current execution date.
        Takes into account "verweving"

        Args:
            None

        Returns:
            None
        """
        execution_date = datetime.now()
        self.typeURI_mapping_dict = {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Wegkantkast": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HSCabine": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Laagspanningsbord": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSBordLegacy",
            "lgc:installatie#HS": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS",
            "lgc:installatie#HSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel",
            "lgc:installatie#LS": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
            "lgc:installatie#LSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB",
            "IP": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkelement",
            "TT": "https://lgc.data.wegenenverkeer.be/ns/installatie#TT",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Segmentcontroller": "https://lgc.data.wegenenverkeer.be/ns/installatie#SegC",
            "lgc:installatie#WV": "https://lgc.data.wegenenverkeer.be/ns/installatie#WV",
            "lgc:installatie#IP": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkelement",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule": "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule",
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVMeetpunt": "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVMeetpunt",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal",
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Cabinecontroller": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Cabinecontroller"
        }
        # Update URI's na specifieke verwevingsdatum
        if execution_date > datetime(year=2025, month=6, day=13):
            # RVMS
            self.typeURI_mapping_dict[
                'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS'
            self.typeURI_mapping_dict[
                'lgc:installatie#RVMSGroep'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep'
            # VMS
            self.typeURI_mapping_dict[
                'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS'
            self.typeURI_mapping_dict[
                'lgc:installatie#VMSGroep'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep'
        if execution_date > datetime(year=2025, month=6, day=17):
            self.typeURI_mapping_dict[
                'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS'
            self.typeURI_mapping_dict[
                'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep'] = 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep'

    def setup_mapping_dict_eigenschappen(self):
        """
        Sets up the mapping dictionary for properties (eigenschappen).

        Args:
            None

        Returns:
            None
        """
        self.eigenschappen_mapping_dict = {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning": [
                {
                    "eigenschap_naam": "eanNummer",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#DNB.eanNummer"
                },
                {
                    "eigenschap_naam": "referentieDNB",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#DNB.referentieDNB"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB": [
                {
                    "eigenschap_naam": "meternummer",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Energiemeter.meternummer"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule": [
                {
                    "eigenschap_naam": "type",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule.type"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVMeetpunt": [
                {
                    "eigenschap_naam": "uitslijprichting",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVMeetpunt.uitslijprichting"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera": [
                {
                    "eigenschap_naam": "isPtz",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera.isPtz"
                },
                {
                    "eigenschap_naam": "heeftAid",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera.heeftAid"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS": [
                {
                    "eigenschap_naam": "merk",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS.merk"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS": [
                {
                    "eigenschap_naam": "merk",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS.merk"
                }
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug": [
                {
                    "eigenschap_naam": "vrijeHoogte",
                    "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug.vrijeHoogte"
                }
            ]
        }

    def setup_logging(self):
        """
        Sets up logging configuration for the LantisBypass class.

        Args:
            None

        Returns:
            None
        """
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
        self.df_assets_wegkantkasten = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                     sheet_name="Wegkantkasten")
        self.df_assets_voeding = self.import_data_as_dataframe(filepath=self.excel_file,
                                                               sheet_name="HSCabines-CC-SC-HS-LS-Switch-WV")
        self.df_assets_openbare_verlichting = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                            sheet_name="Openbare verlichting")
        self.df_assets_mivlve = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="MIVLVE")
        self.df_assets_mivmeetpunten = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                     sheet_name="MIVMeetpunten")
        self.df_assets_RSS_borden = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="RSS-borden")
        self.df_assets_RVMS_borden = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="(R)VMS-borden")
        self.df_assets_cameras = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="Cameras")
        self.df_assets_portieken_seinbruggen = self.import_data_as_dataframe(filepath=self.excel_file,
                                                                             sheet_name="Portieken-Seinbruggen")
        self.df_assets_galgpaal = self.import_data_as_dataframe(filepath=self.excel_file, sheet_name="Galgpaal")

    def process_installatie(self, df: pd.DataFrame, column_name: str, asset_type: AssetType) -> None:
        """
        Process installations (installaties) for a specific asset type based on the provided DataFrame.
        Args:
            df (pd.DataFrame): The DataFrame containing asset data.
            column_name (str): The name of the column in the DataFrame.
            asset_type (AssetType): The type of the asset.

        Returns:
            None
        """
        logging.info(f'Aanmaken van installaties bij het assettype: {asset_type}')
        for idx, asset_row in df.iterrows():
            asset_row_naam = asset_row.get(column_name)
            installatie_naam = self.construct_installatie_naam(naam=asset_row_naam, asset_type=asset_type)
            df.at[idx, "installatie_naam"] = installatie_naam
            df.at[idx, "installatie_uuid"] = self.create_installatie_if_missing(naam=installatie_naam)

        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'Beheerobject_{asset_type.value}',
                        columns=["installatie_uuid", "installatie_naam"],
                        index=False, freeze_panes=[1, 1])
        logging.info(f'Installaties bij het assettype {asset_type} aangemaakt')

    # Helper function for lookups
    def _search_parent_asset_by_uuid(self, uuid: str):
        if uuid:
            return next(self.eminfra_client.search_asset_by_uuid(asset_uuid=uuid), None)
        return None

    def _search_parent_beheerobject_by_uuid(self, uuid: str):
        if uuid:
            return self.eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=uuid)
        return None

    def _search_parent_asset_by_name(self, name: str):
        if name:
            return next(self.eminfra_client.search_asset_by_name(asset_name=name, exact_search=True), None)
        return None

    def _search_parent_beheerobject_by_name(self, name: str):
        if name:
            return next(self.eminfra_client.search_beheerobjecten(naam=name, operator=OperatorEnum.EQ), None)
        return None

    def process_assets(self
                       , df: pd.DataFrame
                       , asset_info: AssetInfo
                       , parent_asset_info: ParentAssetInfo = None
                       , eigenschap_infos: [EigenschapInfo] = None
                       , add_geometry: bool = False
                       , steun_relatie_uri: str = None
                       , relatie_infos: [RelatieInfo] = None
                       , sheetname_prefix: str = 'K') -> None:
        """
        Generieke functie voor het verwerken van een dataframe van assets.
        
        :param df: dataframe
        :param asset_info: AssetInfo object
        :param parent_asset_info: ParentAssetInfo object
        :param eigenschap_infos: Lijst met EigenschapInfo objecten
        :param add_geometry: Boolean. Aanduiding of geometrie dient te worden aangemaakt.
        :param steun_relatie_uri: string. De geometrie dient via de steun-relatie te worden afgeleid op basis van de relatie_uri
        :param relatie_infos: Lijst met EigenschapInfo objecten
        :param sheetname_prefix: Prefix voor de naam van de Excel sheet ("K" voor Kast of "HS" voor HSCabine)
        :return: 
        """
        logging.info(f'Aanmaken van assets ... (assettype: {asset_info.asset_type.value}) ')

        if relatie_infos is None:
            relatie_infos = []
        if eigenschap_infos is None:
            eigenschap_infos = []

        df_output_columns = []
        df_output_columns.append(asset_info.column_name)

        for idx, asset_row in df.iterrows():
            if asset_info.column_asset_aanwezig and asset_row.get(asset_info.column_asset_aanwezig) and asset_row.get(
                    asset_info.column_asset_aanwezig).lower() == 'nee':
                continue  # skip this cycle of the for-loop.

            asset = None

            asset_row_uuid = asset_row.get(asset_info.column_uuid)
            if asset_info.column_typeURI.startswith('https://') or asset_info.column_typeURI.startswith('lgc:'):
                asset_row_typeURI = asset_info.column_typeURI
            else:
                asset_row_typeURI = asset_row.get(asset_info.column_typeURI)
            typeURI = self.typeURI_mapping_dict.get(asset_row_typeURI, asset_row_typeURI)
            asset_row_name = asset_row.get(asset_info.column_name, None)

            logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

            if not asset_row_name:
                logging.debug('Asset lacks a name. Continuing with the next asset.')
                continue

            if asset_row_uuid and asset_row_name:
                logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
                self.validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

            if parent_asset_info:  # Legacy
                # Default to None
                parent_asset = None

                # Try UUID lookup first
                if parent_asset_info.column_parent_uuid:
                    parent_uuid = asset_row.get(parent_asset_info.column_parent_uuid)
                    if parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.ASSET:
                        parent_asset = self._search_parent_asset_by_uuid(uuid=parent_uuid)
                    elif parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.BEHEEROBJECT:
                        parent_asset = self._search_parent_beheerobject_by_uuid(uuid=parent_uuid)

                elif parent_asset_info.column_parent_name:
                    parent_name = asset_row.get(parent_asset_info.column_parent_name)
                    if parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.ASSET:
                        parent_asset = self._search_parent_asset_by_name(name=parent_name)
                    elif parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.BEHEEROBJECT:
                        parent_asset = self._search_parent_beheerobject_by_name(name=parent_name)

                elif asset_row_name:
                    installatie_name = self.construct_installatie_naam(naam=asset_row_name,
                                                                       asset_type=asset_info.asset_type)
                    if parent_asset_info.parent_asset_type == BoomstructuurAssetTypeEnum.BEHEEROBJECT:
                        parent_asset = next(
                            self.eminfra_client.search_beheerobjecten(naam=installatie_name, actief=True,
                                                                      operator=OperatorEnum.EQ),
                            None
                        )

                # Maak asset (Legacy of OTL) of basis van de typeURI en de parent-asset
                if parent_asset is None:
                    logging.critical(f'Parent asset is ongekend. Legacy of OTL-asset kon niet aangemaakt worden voor '
                                     f'assettype: {typeURI}.')
                    df.at[idx, "asset_uuid"] = "UUID of parent asset is missing"
                else:
                    asset = self.create_asset_if_missing(typeURI=typeURI, asset_naam=asset_row_name,
                                                         parent_uuid=parent_asset.uuid,
                                                         parent_asset_type=parent_asset_info.parent_asset_type)

            if asset:
                df.at[idx, "asset_uuid"] = asset.uuid
                df_output_columns.insert(0, "asset_uuid")

                # Aanmaken van eigenschappen
                for eigenschap_info in eigenschap_infos:
                    eigenschapwaarde_nieuw = str(asset_row.get(eigenschap_info.column_eigenschap_name)) # Cast to a string to handle the value 'False'
                    if eigenschapwaarde_nieuw: # Not None
                        logging.debug(f'process asset: "{asset.uuid}", update eigenschap "{eigenschap_info.eminfra_eigenschap_name}" with value "{eigenschapwaarde_nieuw}".')
                        self.update_eigenschap(asset=asset, eigenschapnaam_bestaand=eigenschap_info.eminfra_eigenschap_name,
                                               eigenschapwaarde_nieuw=eigenschapwaarde_nieuw)
                    else:
                        logging.debug(f'Eigenschap "{eigenschap_info.eminfra_eigenschap_name}" heeft een lege waarde en wordt niet geüpdatet.')

                # Aanmaken van relaties
                for relatie_info in relatie_infos:
                    # uri aanwezig in Excel-file
                    if relatie_info.column_typeURI_relatie and asset_row.get(relatie_info.column_typeURI_relatie):
                        relatie_descriptive_naam = relatie_info.uri.value.split('#')[-1]
                        bronAsset_uuid = asset_row.get(relatie_info.bronAsset_uuid, asset.uuid)
                        bronAsset = self.eminfra_client.get_asset_by_id(bronAsset_uuid)
                        doelAsset_uuid = asset_row.get(relatie_info.doelAsset_uuid, asset.uuid)
                        doelAsset = self.eminfra_client.get_asset_by_id(doelAsset_uuid)
                        relatie = map_relatie(relatie_info.uri.value)
                        assetrelatie = create_relatie_if_missing(client=self.eminfra_client,
                                                                 bron_asset=bronAsset,
                                                                 doel_asset=doelAsset,
                                                                 relatie=relatie)
                        # append relatie_uuid to the dataframe
                        df.at[idx, f'relatie_uuid_{relatie_descriptive_naam}'] = assetrelatie.uuid
                        df_output_columns.append(f'relatie_uuid_{relatie_descriptive_naam}')

                # Toevoegen van de geometrie op basis van absolute coördinaten
                if add_geometry:
                    if wkt_geometry := self.parse_wkt_point_geometry(asset_row=asset_row):
                        logging.info("Coordinates available. Parse WKT and set WKT-string as geometry.")
                        if typeURI.startswith('https://lgc.'):
                            logging.debug(f'Update eigenschap locatie (Legacy): "{asset.uuid}": "{wkt_geometry}"')
                            self.eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid,
                                                                                     wkt_geom=wkt_geometry)
                        elif typeURI.startswith('https://wegenenverkeer.data.vlaanderen.be'):
                            logging.debug(f'Update eigenschap geometrie (OTL): "{asset.uuid}": "{wkt_geometry}"')
                            self.eminfra_client.update_geometrie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geometry=wkt_geometry)

                # Toevoegen van de geometrie op basis van de steun-relatie
                if steun_relatie_uri:
                    relatie = map_relatie(relatie_uri=steun_relatie_uri)
                    self.set_geometrie_via_steun_relatie(asset=asset, relatie=relatie)

                # Update toestand
                if asset_info.column_status:
                    # default waarde "in-opbouw" indien er geen waarde is ingevuld.
                    nieuwe_status = asset_row.get(asset_info.column_status)
                    if nieuwe_status is None:
                        nieuwe_toestand = AssetDTOToestand.IN_OPBOUW
                    else:
                        nieuwe_toestand = map_status(nieuwe_status)
                else:
                    nieuwe_toestand = AssetDTOToestand.IN_OPBOUW
                huidige_toestand = asset.toestand
                if nieuwe_toestand != huidige_toestand:
                    self.eminfra_client.update_toestand(asset=asset, toestand=nieuwe_toestand)

                # Bestekkoppelingen
                self.add_bestekkoppeling_if_missing(asset_uuid=asset.uuid,
                                                    eDelta_dossiernummer=self.eDelta_dossiernummer,
                                                    start_datetime=self.start_datetime)

                # Toezichter (LANTIS) toewijzen
                # Toezichtsgroep (LANTIS) toewijzen
                self.add_toezichter_if_missing(asset=asset)

                # Schadebeheerder (LANTIS) toewijzen
                self.add_schadebeheerder_if_missing(asset=asset)

        # Wegschrijven van het dataframe
        with pd.ExcelWriter(self.output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'{sheetname_prefix}_{asset_info.asset_type.value}',
                        columns=list(dict.fromkeys(df_output_columns)), index=False, freeze_panes=[1, 1])
        logging.info(f'Assets aangemaakt (assettype: {asset_info.asset_type.value})')

    def process_wegkantkasten(self):
        logging.info('Aanmaken Wegkantkasten')
        asset_info = AssetInfo(asset_type=AssetType.WEGKANTKAST, column_typeURI='Wegkantkast_Object typeURI',
                               column_uuid='Wegkantkast_UUID Object',
                               column_name='Wegkantkast_Object assetId.identificator',
                               column_status='Wegkantkast_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)
        bypass.process_assets(df=bypass.df_assets_wegkantkasten, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, add_geometry=True, sheetname_prefix='Kast')

    def process_wegkantkasten_lsdeel(self):
        logging.info('Aanmaken LSDeel')
        asset_info = AssetInfo(asset_type=AssetType.LSDEEL, column_uuid='Laagspanningsdeel_UUID LSDeel',
                               column_name='Laagspanningsdeel_Naam LSDeel',
                               column_typeURI='https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel',
                               column_asset_aanwezig='Laagspanningsdeel_LSDeel aanwezig',
                               column_status='Laagspanningsdeel_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='Wegkantkast_UUID Object',
                                            column_parent_name='Wegkantkast_Object assetId.identificator')
        bevestigingsrelatie = RelatieInfo(bronAsset_uuid='Laagspanningsdeel_UUID LSDeel'
                                          , doelAsset_uuid='Wegkantkast_UUID Object'
                                          , uri=RelatieEnum.BEVESTIGING
                                          , column_typeURI_relatie='Bevestigingsrelatie LSDeel_Bevestigingsrelatie typeURI')
        voedingsrelatie = RelatieInfo(bronAsset_uuid='Voedingsrelatie (oorsprong)_UUID Voedingsrelatie bronAsset'
                                      , doelAsset_uuid='Laagspanningsdeel_UUID LSDeel'
                                      , uri=RelatieEnum.VOEDT
                                      , column_typeURI_relatie='Voedingsrelatie (oorsprong)_Voedingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_wegkantkasten, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
                              relatie_infos=[bevestigingsrelatie, voedingsrelatie], sheetname_prefix='Kast')

    def process_voeding_HS_cabine(self):
        logging.info('Aanmaken HSCabine')
        asset_info = AssetInfo(asset_type=AssetType.HSCABINE, column_name='HSCabine_Object assetId.identificator',
                               column_uuid='HSCabine_UUID Object', column_typeURI='HSCabine_Object typeURI',
                               column_status='HSCabine_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid=None, column_parent_name=None)
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              add_geometry=True, sheetname_prefix='HS')

    def process_voeding_hoogspanningsdeel(self):
        logging.info('Aanmaken HSDeel')
        asset_info = AssetInfo(asset_type=AssetType.HSDEEL, column_name='Hoogspanningsdeel_Naam HSDeel',
                               column_uuid='Hoogspanningsdeel_UUID HSDeel',
                               column_typeURI='Hoogspanningsdeel_HSDeel lgc:installatie',
                               column_asset_aanwezig='Hoogspanningsdeel_HSDeel aanwezig (Ja/Nee)',
                               column_status='Hoogspanningsdeel_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='HSCabine_UUID Object',
                                            column_parent_name='HSCabine_Object assetId.identificator')
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='HSCabine_UUID Object',
                                          column_typeURI_relatie='Bevestigingsrelatie HSDeel_Bevestigingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', relatie_infos=[bevestigingsrelatie], sheetname_prefix='HS')

    def process_voeding_laagspanningsdeel(self):
        logging.info('Aanmaken LSDeel')
        asset_info = AssetInfo(asset_type=AssetType.LSDEEL, column_name='Laagspanningsdeel_Naam LSDeel',
                               column_uuid='Laagspanningsdeel_UUID LSDeel',
                               column_typeURI='Laagspanningsdeel_LSDeel lgc:installatie',
                               column_asset_aanwezig='Laagspanningsdeel_LSDeel aanwezig (Ja/Nee)',
                               column_status='Laagspanningsdeel_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='HSCabine_UUID Object',
                                            column_parent_name='HSCabine_Object assetId.identificator')
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='HSCabine_UUID Object',
                                          column_typeURI_relatie='Bevestigingsrelatie LSDeel_Bevestigingsrelatie typeURI')
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT, bronAsset_uuid='Hoogspanningsdeel_UUID HSDeel',
                                      doelAsset_uuid=None,
                                      column_typeURI_relatie='Voedingsrelatie HSDeel naar LSDeel_Voedingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', relatie_infos=[bevestigingsrelatie, voedingsrelatie],
                              sheetname_prefix='HS')

    def process_voeding_hoogspanning(self):
        logging.info('Aanmaken Hoogspanning')
        asset_info = AssetInfo(asset_type=AssetType.HS, column_name='Hoogspanning_Naam HS',
                               column_uuid='Hoogspanning_UUID HS', column_typeURI='Hoogspanning_HS lgc:installatie',
                               column_asset_aanwezig='Hoogspanning_HS aanwezig (Ja/Nee)',
                               column_status='Hoogspanning_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='HSCabine_UUID Object',
                                            column_parent_name='HSCabine_Object assetId.identificator')
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='HSCabine_UUID Object',
                                          column_typeURI_relatie='Bevestigingsrelatie HS_Bevestigingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', relatie_infos=[bevestigingsrelatie], sheetname_prefix='HS')

    def process_voeding_DNBHoogspanning(self):
        logging.info('Aanmaken DNBHoogspanning')
        asset_info = AssetInfo(asset_type=AssetType.DNBHOOGSPANNING,
                               column_name='DNBHoogspanning_Object assetId.identificator',
                               column_uuid='DNBHoogspanning_UUID Object',
                               column_typeURI='DNBHoogspanning_Object typeURI',
                               column_status='DNBHoogspanning_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='Hoogspanning_UUID HS',
                                            column_parent_name=None)
        eigenschappen = [
            EigenschapInfo(eminfra_eigenschap_name='eanNummer', column_eigenschap_name='DNBHoogspanning_eanNummer')]
        eigenschappen.append(EigenschapInfo(eminfra_eigenschap_name='referentieDNB',
                                            column_eigenschap_name='DNBHoogspanning_referentieDNB'))
        hoortbijrelatie = RelatieInfo(uri=RelatieEnum.HOORTBIJ, bronAsset_uuid=None,
                                      doelAsset_uuid='Hoogspanning_UUID HS',
                                      column_typeURI_relatie='HoortBij Relatie voor DNBHoogspanning_HoortBij typeURI')  # Hoortbij relatie van OTL naar Legacy-asset
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              eigenschap_infos=eigenschappen, relatie_infos=[hoortbijrelatie],
                              sheetname_prefix='HS')

    def process_voeding_energiemeter_DNB(self):
        logging.info('Aanmaken EnergiemeterDNB')
        asset_info = AssetInfo(asset_type=AssetType.ENERGIEMETERDNB,
                               column_name='EnergiemeterDNB_Object assetId.identificator',
                               column_uuid='EnergiemeterDNB_UUID Object',
                               column_typeURI='EnergiemeterDNB_Object typeURI',
                               column_status='EnergiemeterDNB_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='Hoogspanning_UUID HS',
                                            column_parent_name=None)
        eigenschappen = [EigenschapInfo(eminfra_eigenschap_name='meternummer', column_eigenschap_name='meternummer')]
        hoortbijrelatie = RelatieInfo(uri=RelatieEnum.HOORTBIJ,
                                      bronAsset_uuid=None,
                                      doelAsset_uuid='Hoogspanning_UUID HS',
                                      column_typeURI_relatie='HoortBij Relatie voor EnergiemeterDNB_HoortBij typeURI')  # Hoortbij relatie van OTL naar Legacy-asset
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              eigenschap_infos=eigenschappen, relatie_infos=[hoortbijrelatie],
                              sheetname_prefix='HS')

    def process_voeding_cabinecontroller(self):
        logging.info('Aanmaken CabineController')
        asset_info = AssetInfo(asset_type=AssetType.CABINECONTROLLER, column_name='CabineController_Naam CC',
                               column_uuid='CabineController_UUID CC', column_typeURI='CabineController_CC TypeURI',
                               column_status='CabineController_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid=None, column_parent_name=None)
        bevestigingrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING,
                                      bronAsset_uuid=None,
                                      doelAsset_uuid='HSCabine_UUID Object',
                                      column_typeURI_relatie='Bevestiging Relatie voor CabineController_Bevestiging typeURI')
        # Afgeleide locatie is niet toegestaan tussen CabineController (OTL) en HSCabine (Legacy)
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              relatie_infos=[bevestigingrelatie],
                              # steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
                              sheetname_prefix='HS')

    def process_voeding_segmentcontroller(self):
        logging.info('Aanmaken SegmentController')
        asset_info = AssetInfo(asset_type=AssetType.SEGC, column_name='Segmentcontroller_Naam SC',
                               column_uuid='Segmentcontroller_UUID SC', column_typeURI='Segmentcontroller_SC TypeURI',
                               column_status='SegmentController_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid=None, column_parent_name=None)
        bevestigingrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING,
                                      bronAsset_uuid=None,
                                      doelAsset_uuid='HSCabine_UUID Object',
                                      column_typeURI_relatie='Bevestiging Relatie voor SegmentController_Bevestiging typeURI')
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              relatie_infos=[bevestigingrelatie],
                              steun_relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging', sheetname_prefix='HS')

    def process_voeding_wegverlichtingsgroep(self):
        logging.info('Aanmaken Wegverlichtingsgroep')
        asset_info = AssetInfo(asset_type=AssetType.WVGROEP, column_name='Wegverlichtingsgroep_Naam WV',
                               column_uuid='Wegverlichtingsgroep_UUID WV',
                               column_typeURI='Wegverlichtingsgroep_WV lgc:installatie',
                               column_asset_aanwezig='Wegverlichtingsgroep_WV aanwezig (Ja/Nee)',
                               column_status='Wegverlichtingsgroep_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid=None, column_parent_name=None)
        bypass.process_assets(df=bypass.df_assets_voeding, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              sheetname_prefix='HS')

    def process_openbare_verlichting(self):
        logging.info('Aanmaken WVLichtmast')
        asset_info = AssetInfo(asset_type=AssetType.WVLICHTMAST, column_name='WVLichtmast_Object assetId.identificator',
                               column_uuid='WVLichtmast_UUID Object', column_typeURI='WVLichtmast_Object typeURI',
                               column_status='WVLichtmast_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='parent_asset_uuid', column_parent_name=None)
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT,
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset',
                                      doelAsset_uuid=None,
                                      column_typeURI_relatie='Voedingsrelaties_Voedingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_openbare_verlichting, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, relatie_infos=[voedingsrelatie], add_geometry=True,
                              sheetname_prefix='HS')

    def process_mivlve(self):
        logging.info('Aanmaken Meetlussen MIVLVE')
        asset_info = AssetInfo(asset_type=AssetType.MIVLVE, column_uuid='LVE_UUID Object',
                               column_name='LVE_Object assetId.identificator', column_typeURI='LVE_Object typeURI',
                               column_status='LVE_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset',
                                            column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator')

        eigenschap_infos = [
            EigenschapInfo(eminfra_eigenschap_name='type', column_eigenschap_name='LVE_Type')]
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT,
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset',
                                      doelAsset_uuid=None,
                                      column_typeURI_relatie='Voedingsrelaties_Voedingsrelatie typeURI')
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset',
                                          column_typeURI_relatie='Bevestigingsrelatie_Bevestigingsrelatie typeURI')
        sturingsrelatie = RelatieInfo(uri=RelatieEnum.STURING, bronAsset_uuid=None,
                                      doelAsset_uuid='Netwerkgegevens_UUID Poort',
                                      column_typeURI_relatie='Sturingsrelaties_Sturingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_mivlve, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              eigenschap_infos=eigenschap_infos, add_geometry=True,
                              relatie_infos=[voedingsrelatie, bevestigingsrelatie, sturingsrelatie],
                              sheetname_prefix='MIVLVE')

    def process_mivmeetpunten(self):
        logging.info('Aanmaken Meetpunten')
        asset_info = AssetInfo(asset_type=AssetType.MPT, column_uuid='Meetpunt_UUID Object',
                               column_name='Meetpunt_Object assetId.identificator',
                               column_typeURI='Meetpunt_Object typeURI',
                               column_status='Meetpunt_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_uuid='Sturingsrelaties_UUID Sturingsrelatie bronAsset',
                                            column_parent_name='Sturingsrelatie_Sturingsrelatie bron AssetId.identificator')
        # todo: activeer de eigenschap zodra ingevuld in Excel input-file
        # eigenschap_infos = [
        #     EigenschapInfo(eminfra_eigenschap_name='uitslijprichting',
        #                    column_eigenschap_name='Meetpunt_Uitslijprichting')
            # , EigenschapInfo(eminfra_eigenschap_name='aansluiting', column_eigenschap_name='Meetpunt_Aansluiting')
            # , EigenschapInfo(eminfra_eigenschap_name='wegdek', column_eigenschap_name='Meetpunt_Wegdek')
        # ]
        sturingsrelatie = RelatieInfo(uri=RelatieEnum.STURING,
                                      bronAsset_uuid=None,
                                      doelAsset_uuid='Sturingsrelaties_UUID Sturingsrelatie bronAsset',
                                      column_typeURI_relatie='Sturingsrelaties_Sturingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_mivmeetpunten, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, add_geometry=True, relatie_infos=[sturingsrelatie],
                              sheetname_prefix='MIVLVE')

    def process_camera(self):
        logging.info('Aanmaken Camera')
        logging.debug('Camgroep onder 1 installatie plaatsen')
        asset_info = AssetInfo(asset_type=AssetType.CAMERA,
                               column_typeURI='Camera_Object typeURI',
                               column_name='Camera_Object assetId.identificator', column_uuid='Camera_UUID Object',
                               column_status='Camera_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_name=None,
                                            column_parent_uuid='parent_beheerobject_uuid')
        eigenschap_infos = [EigenschapInfo(eminfra_eigenschap_name='isPtz', column_eigenschap_name='Camera_isPtz')]
        eigenschap_infos.append(EigenschapInfo(eminfra_eigenschap_name='heeftAid', column_eigenschap_name='Camera_heeftAid'))
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT,
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset',
                                      doelAsset_uuid='Camera_UUID Object',
                                      column_typeURI_relatie='Voedingsrelaties_Voedingsrelatie typeURI')
        # tijdelijk on-hold.
        # bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING,
        #                                   bronAsset_uuid=None,
        #                                   doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset',
        #                                   column_typeURI_relatie='Bevestigingsrelatie_Bevestigingsrelatie typeURI')
        sturingsrelatie = RelatieInfo(uri=RelatieEnum.STURING,
                                      bronAsset_uuid=None,
                                      doelAsset_uuid='Netwerkgegevens_UUID Poort',
                                      column_typeURI_relatie='Sturingsrelaties_Sturingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_cameras, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              eigenschap_infos=eigenschap_infos, add_geometry=True,
                              relatie_infos=[
                                  voedingsrelatie,
                                  # bevestigingsrelatie,
                                  sturingsrelatie
                              ], sheetname_prefix='Camera')

    def process_RSS_borden(self):
        logging.info('Aanmaken RSSBord')
        asset_info = AssetInfo(asset_type=AssetType.RSSBORD,
                               column_typeURI='DVM-Bord_Object typeURI',
                               column_name='DVM-Bord_Object assetId.identificator', column_uuid='DVM-Bord_UUID Object',
                               column_status='DVM-Bord_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_name='HoortBij Relatie voor RSS-groep_HoortBij doelAssetId.identificator',
                                            column_parent_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset')
        # todo: Activeer de eigenschap na de verweving. De eigenschap "merk" is pas beschikbaar na verweving
        eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='merk', column_eigenschap_name='DVM-Bord_merk')
        hoortbijrelatie = RelatieInfo(uri=RelatieEnum.HOORTBIJ, bronAsset_uuid=None,
                                      doelAsset_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset',
                                      column_typeURI_relatie='HoortBij Relatie voor RSS-groep_HoortBij typeURI')
        # verwissel bron en doel uuid, want bevestiging is een bidirectionele relatie
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset',
                                          column_typeURI_relatie='Bevestigingsrelatie_Bevestigingsrelatie typeURI')
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT,
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset',
                                      doelAsset_uuid=None,
                                      column_typeURI_relatie='Voedingsrelaties_Voedingsrelatie typeURI')
        sturingsrelatie = RelatieInfo(uri=RelatieEnum.STURING, bronAsset_uuid=None,
                                      doelAsset_uuid='Netwerkgegevens_UUID Poort',
                                      column_typeURI_relatie='Sturingsrelatie_Sturingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_RSS_borden, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, eigenschap_infos=[eigenschap_info],
                              add_geometry=True,
                              relatie_infos=[hoortbijrelatie, bevestigingsrelatie, voedingsrelatie, sturingsrelatie],
                              sheetname_prefix='Seinbrug_RSS')

    def process_RVMS_borden(self):
        logging.info('Aanmaken RVMS-Bord')
        asset_info = AssetInfo(asset_type=AssetType.RVMSBORD,
                               column_typeURI='DVM-Bord_Object typeURI',
                               column_name='DVM-Bord_Object assetId.identificator', column_uuid='DVM-Bord_UUID Object',
                               column_status='DVM-Bord_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_name='HoortBij Relatie DynBordGroep_HoortBij doelAssetId.identificator',
                                            column_parent_uuid='HoortBij Relatie DynBordGroep_UUID HoortBij doelAsset')
        # todo: Activeer de eigenschap na de verweving. De eigenschap "merk" is pas beschikbaar na verweving
        # eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='merk', column_eigenschap_name='DVM-Bord_merk')
        hoortbijrelatie = RelatieInfo(uri=RelatieEnum.HOORTBIJ, bronAsset_uuid=None,
                                      doelAsset_uuid='HoortBij Relatie DynBordGroep_UUID HoortBij doelAsset',
                                      column_typeURI_relatie='HoortBij Relatie DynBordGroep_HoortBij typeURI')
        bevestigingsrelatie = RelatieInfo(uri=RelatieEnum.BEVESTIGING, bronAsset_uuid=None,
                                          doelAsset_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset',
                                          column_typeURI_relatie='Bevestigingsrelatie_Bevestigingsrelatie typeURI')
        voedingsrelatie = RelatieInfo(uri=RelatieEnum.VOEDT,
                                      bronAsset_uuid='Voedingsrelaties_UUID Voedingsrelatie bronAsset',
                                      doelAsset_uuid=None,
                                      column_typeURI_relatie='Voedingsrelaties_Voedingsrelatie typeURI')
        sturingsrelatie = RelatieInfo(uri=RelatieEnum.STURING, bronAsset_uuid=None,
                                      doelAsset_uuid='Netwerkgegevens_UUID Poort',
                                      column_typeURI_relatie='Sturingsrelatie_Sturingsrelatie typeURI')
        bypass.process_assets(df=bypass.df_assets_RVMS_borden, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, add_geometry=True,
                              relatie_infos=[hoortbijrelatie, bevestigingsrelatie, voedingsrelatie, sturingsrelatie],
                              sheetname_prefix='Seinbrug_RVMS')

    def process_seinbruggen(self):
        logging.info('Aanmaken Seinbrug DVM')
        asset_info = AssetInfo(asset_type=AssetType.SEINBRUG, column_typeURI='Seinbrug_Object typeURI',
                               column_uuid='Seinbrug_UUID Object', column_name='Seinbrug_Object assetId.identificator',
                               column_status='Seinbrug_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid='parent_beheerobject_uuid')
        # todo Activeer de eigenschap na de verweving. De eigenschap "vrije hoogte" is pas beschikbaar na verweving van Seinbrug naar OTL.
        # eigenschap_info = EigenschapInfo(eminfra_eigenschap_name='vrije hoogte',
        #                                  column_eigenschap_name='Seinbrug_vrijeHoogte')
        bypass.process_assets(df=bypass.df_assets_portieken_seinbruggen, asset_info=asset_info,
                              parent_asset_info=parent_asset_info, add_geometry=True, sheetname_prefix='Seinbrug')

    def import_data_as_dataframe(self, filepath: Path, sheet_name: str = None):
        """
        Imports data from an Excel file into a Pandas DataFrame, validates the data structure, and returns the DataFrame.

        Args:
            filepath (Path): The path to the Excel file.
            sheet_name (str, optional): The name of the sheet in the Excel file. Defaults to None.

        Returns:
            pd.DataFrame: The DataFrame containing the imported data.
        Raises:
            ValueError: If the validation of the DataFrame structure fails.
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

    def create_asset_if_missing(self, typeURI: str, asset_naam: str, parent_uuid: str,
                                parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT) -> AssetDTO | None:
        """
        Maak de asset aan indien nog onbestaande en geef de asset terug

        :param typeURI: asset typeURI
        :asset_naam: asset naam
        :parent_uuid: parent uuid
        :parent_uuid: parent uuid
        :parent_asset_type:
        :return: asset
        """
        asset = None
        assettype = self.eminfra_client.search_assettype(uri=typeURI)
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(expressions=[
                ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype.uuid}')]),
                ExpressionDTO(terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)],
                              logicalOp=LogicalOpEnum.AND),
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
                typeUuid=assettype.uuid,
                parent_asset_type=parent_asset_type)
            asset = next(self.eminfra_client.search_asset_by_uuid(asset_uuid=asset_dict.get('uuid')), None)

        else:
            logging.critical('Unknown error')
            raise ValueError(
                f'Could not create new asset. typeURI: {typeURI}, asset_naam: {asset_naam}, parent_uuid: {parent_uuid}')

        return asset


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
        mapping_exceptions = {
            "A13M0.50.K": "A13X0.4"
        }
        try:
            return mapping_exceptions[naam]
        except KeyError:
            logging.info("Strip kast naam naar installatie naam.")

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
        """
        Bevat ook de uitzondering .LSBord

        :param naam:
        :return:
        """
        if naam.endswith('.HSCab'):
            installatie_naam = naam[:-6]
        elif naam.endswith('.LSBord'):
            installatie_naam = naam[:-7]
        else:
            raise ValueError(
                f"De naam van de HSCabine ({naam}) eindigt niet op '.HSCab of '.LSBord' (uitzondering)")
        return installatie_naam

    def _construct_installatie_naam_hoogspanningsdeel(self, naam: str) -> str:
        if naam.endswith('.HSDeel'):
            installatie_naam = naam.replace('.HSDeel', '.HSCab')
        else:
            raise ValueError(f"De naam van het Hoogspanningsdeel ({naam}) eindigt niet op '.HSDeel'")
        return installatie_naam

    def _construct_installatie_naam_laagspanningsdeel(self, naam: str) -> str:
        if naam.endswith('.LSDeel'):
            installatie_naam = naam.replace('.LSDeel', '.HSCab')
        else:
            raise ValueError(f"De naam van het Laagspanningsdeel ({naam}) eindigt niet op '.LSDeel'")
        return installatie_naam

    def _construct_installatie_naam_hoogspanning(self, naam: str) -> str:
        if naam.endswith('.HS'):
            installatie_naam = naam.replace('.HS', '.HSCab')
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

        if match := re.search(r'(.*)([MPN])(?!.*[MPN])', installatie_naam):
            installatie_naam = match[1] + 'X' + installatie_naam[match.end():]
        else:
            raise ValueError("De syntax van de asset bevat geen letter 'P', 'N' of 'M'.")
        return installatie_naam

    def _construct_installatie_naam_cabinecontroller(self, naam):
        if naam.endswith('.CC1'):
            installatie_naam = naam[:-4]
        else:
            raise ValueError(f"De naam van de CabineController ({naam}) eindigt niet op '.CC1'")
        return installatie_naam


    def construct_installatie_naam(self, naam: str, asset_type: AssetType) -> str:
        # kastnaam: str = None, hscabinenaam: str = None, hoogspanningsdeelnaam: str = None, laagspanningsdeelnaam: str = None, hoogspanningnaam: str = None, segmentcontrollernaam: str = None) -> str:
        """
        Bouw de installatie naam op basis van het asset-type
        Seinbrug en Galgpaal hebben dezelfde syntax.
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
        elif asset_type.name == 'SWITCH':
            installatie_naam = self._construct_installatie_naam_switch(naam=naam)
        elif asset_type.name == 'TT':
            installatie_naam = self._construct_installatie_naam_teletransmissieverbinding(naam=naam)
        elif asset_type.name == 'WVLICHTMAST':
            installatie_naam = self._construct_installatie_naam_wvlichtmast(naam=naam)
        elif asset_type.name == 'SEINBRUG':
            installatie_naam = self._construct_installatie_naam_seinbrug(naam=naam)
        elif asset_type.name == 'GALGPAAL':
            installatie_naam = self._construct_installatie_naam_seinbrug(naam=naam)
        elif asset_type.name == 'CABINECONTROLLER':
            installatie_naam = self._construct_installatie_naam_cabinecontroller(naam=naam)
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

    def parse_wkt_point_geometry(self, asset_row) -> str:
        """
        Parses the Well-Known Text (WKT) Point geometry from the asset row data.

        Args:
            asset_row: The row data containing asset information.

        Returns:
            str: The WKT geometry string representing the asset location.
        Raises:
            ValueError: If the coordinates are outside the boundaries of Belgium.
        """
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
        if asset_row_x <= 14637 or asset_row_x >= 297134 or asset_row_y <= 20909 or asset_row_y >= 246425:
            error_message = f'Coordinates (x,y) "{asset_row_x}, {asset_row_y}" are OUTSIDE the boundaries of Belgium (https://epsg.io/31370).'
            logging.critical(error_message)
            raise ValueError(error_message)
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
    def update_eigenschap(self, asset: AssetDTO, eigenschapnaam_bestaand: str, eigenschapwaarde_nieuw: str) -> None:
        """
        Updates a property of an asset with a new value.

        Args:
            asset (AssetDTO): The asset to update the property for.
            eigenschapnaam_bestaand (str): The existing property name.
            eigenschapwaarde_nieuw (str): The new value for the property.

        Returns:
            None
        Raises:
            ValueError: If the existing property is not found or if multiple properties are found.
        """
        uri = next((item["uri"] for items in self.eigenschappen_mapping_dict.values() for item in items if
                    item.get("eigenschap_naam") == eigenschapnaam_bestaand), None)
        eigenschappen_list = bypass.eminfra_client.search_eigenschappen(eigenschap_naam=eigenschapnaam_bestaand,
                                                                        uri=uri)

        if not eigenschappen_list:
            raise ValueError(
                f'Eigenschap "{eigenschapnaam_bestaand}" not found for assettype: "{uri}".')
        if len(eigenschappen_list) != 1:
            raise ValueError(
                f'Multiple eigenschappen found based on name "{eigenschapnaam_bestaand}" and assettype "{uri}"')
        else:
            eigenschap = eigenschappen_list[0]
        _type = eigenschap.type.get("datatype").get("type").get("_type")
        # Workaround: map 'keuzelijst' naar 'text'
        if _type == 'keuzelijst':
            _type = 'text'

        eigenschap_value_update = EigenschapValueUpdateDTO(
            eigenschap=eigenschap
            , typedValue={
                "value": eigenschapwaarde_nieuw
                , "_type": _type
            }
        )

        self.eminfra_client.update_eigenschap(assetId=asset.uuid, eigenschap=eigenschap_value_update)


    def process_RSS_groep(self):
        logging.info('Aanmaken RSS-Groep')
        asset_info = AssetInfo(asset_type=AssetType.DYNBORDGROEP,
                               column_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep',
                               column_name='HoortBij Relatie voor RSS-groep_HoortBij doelAssetId.identificator',
                               column_uuid='HoortBij Relatie voor RSS-groep_UUID HoortBij doelAsset')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator',
                                            column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
        bypass.process_assets(df=bypass.df_assets_RSS_borden, asset_info=asset_info,
                              parent_asset_info=parent_asset_info,
                              sheetname_prefix='Seinbrug_RSS')  # geen eigenschappen, noch relaties voor de RSS-groep

    def process_RVMS_groep(self):
        logging.info('Aanmaken RVMS-Groep')
        asset_info = AssetInfo(asset_type=AssetType.DYNBORDGROEP,
                               column_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep',
                               column_name='HoortBij Relatie DynBordGroep_HoortBij doelAssetId.identificator',
                               column_uuid='HoortBij Relatie DynBordGroep_UUID HoortBij doelAsset')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.ASSET,
                                            column_parent_name='Bevestigingsrelatie_Bevestigingsrelatie doelAssetId.identificator',
                                            column_parent_uuid='Bevestigingsrelatie_UUID Bevestigingsrelatie doelAsset')
        bypass.process_assets(df=bypass.df_assets_RVMS_borden, asset_info=asset_info,
                              parent_asset_info=parent_asset_info,
                              sheetname_prefix='Seinbrug_RVMS')  # geen eigenschappen, noch relaties voor de RVMS-groep

    def process_galgpaal(self):
        logging.info('Aanmaken Galgpaal')
        asset_info = AssetInfo(asset_type=AssetType.GALGPAAL, column_typeURI='Galgpaal_Object typeURI',
                               column_uuid='Galgpaal_UUID Object', column_name='Galgpaal_Object assetId.identificator',
                               column_status='Galgpaal_Status')
        parent_asset_info = ParentAssetInfo(parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT,
                                            column_parent_uuid='parent_beheerobject_uuid')
        bypass.process_assets(df=bypass.df_assets_galgpaal, asset_info=asset_info, parent_asset_info=parent_asset_info,
                              add_geometry=True, sheetname_prefix='Seinbrug')

    def add_toezichter_if_missing(self, asset: AssetDTO) -> None:
        """
        For both Legacy and OTL-assets.
        Add toezichter and toezichtsgroep.
        :param asset:
        :return:
        """
        if asset.type.uri.startswith('https://lgc.data.wegenenverkeer.be'):
            logging.info('Add kenmerk toezichter (LANTIS) en toezichtsgroep (LANTIS) for Legacy-asset.')
            self.eminfra_client.add_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset.uuid,
                                                                     toezichter_uuid='b234e2b4-383c-4380-acae-49e45189bc10',
                                                                     toezichtgroep_uuid='f421e31c-27f6-486e-843b-5ad245dd613b')
        else:
            logging.info('Add kenmerk toezichter (LANTIS) en toezichtsgroep (LANTIS) for OTL-asset.')
            query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 selection=SelectionDTO(expressions=[
                                     ExpressionDTO(terms=[
                                         TermDTO(property='bronAsset', operator=OperatorEnum.EQ,
                                                 value=f'{asset.uuid}')])]))
            betrokkenerelaties = list(self.eminfra_client.search_betrokkenerelaties(query_dto=query_dto))

            if not [item for item in betrokkenerelaties if
                    item.rol == 'toezichter' and item.doel.get("naam") == 'LANTIS']:
                logging.debug('Toezichter LANTIS toevoegen')
                self.eminfra_client.add_betrokkenerelatie(asset=asset,
                                                          agent_uuid='b3dc8b00-2c34-448e-b178-04489164d778',
                                                          rol='toezichter')
            if not [item for item in betrokkenerelaties if
                    item.rol == 'toezichtsgroep' and item.doel.get("naam") == 'LANTIS']:
                logging.debug('Toezichtsgroep LANTIS toevoegen')
                self.eminfra_client.add_betrokkenerelatie(asset=asset,
                                                          agent_uuid='b3dc8b00-2c34-448e-b178-04489164d778',
                                                          rol='toezichtsgroep')

    def add_schadebeheerder_if_missing(self, asset: AssetDTO) -> None:
        """
        For both Legacy and OTL-assets.
        Add schadebeheerder LANTIS
        :param asset:
        :return:
        """
        if asset.type.uri.startswith('https://lgc.data.wegenenverkeer.be'):
            logging.info('Add schadebeheerder (LANTIS) for Legacy-asset.')
            schadebeheerder_bestaand = self.eminfra_client.get_kenmerk_schadebeheerder_by_asset_uuid(asset_uuid=asset.uuid)
            if not schadebeheerder_bestaand:
                self.eminfra_client.add_kenmerk_schadebeheerder(asset_uuid=asset.uuid, schadebeheerder='LANTIS')
        else:
            logging.info('Add betrokkenerelatie (rol: schadebeheerder) (LANTIS) for OTL-asset.')
            query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 selection=SelectionDTO(expressions=[
                                     ExpressionDTO(terms=[
                                         TermDTO(property='bronAsset', operator=OperatorEnum.EQ,
                                                 value=f'{asset.uuid}')])]))
            betrokkenerelaties = list(self.eminfra_client.search_betrokkenerelaties(query_dto=query_dto))

            if not [item for item in betrokkenerelaties if
                    item.rol == 'schadebeheerder' and item.doel.get("naam") == 'LANTIS']:
                logging.debug('Schadebeheerder LANTIS toevoegen')
                self.eminfra_client.add_betrokkenerelatie(asset=asset,
                                                          agent_uuid='b3dc8b00-2c34-448e-b178-04489164d778',
                                                          rol='schadebeheerder')

    def set_geometrie_via_steun_relatie(self, asset: AssetDTO, relatie: RelatieEnum = RelatieEnum.BEVESTIGING):
        """
        Gebruik de bestaande steun-relatie (Bevestiging) om de geometrie af te leiden."
        :param asset:
        :return:
        """
        kenmerkType_uuid, relatieType_uuid = self.eminfra_client.get_kenmerktype_and_relatietype_id(relatie=relatie)
        bestaande_relaties = self.eminfra_client.search_relaties(
            assetId=asset.uuid
            , kenmerkTypeId=kenmerkType_uuid
            , relatieTypeId=relatieType_uuid
        )
        if relatie := next(bestaande_relaties, None):
            bronAsset_uuid = asset.uuid
            doelAsset_uuid = relatie.uuid
            logging.debug(
                f'''Relatie aanwezig tussen bron-asset ({bronAsset_uuid}) en doel-asset ({doelAsset_uuid})''')
            self.eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=bronAsset_uuid,
                                                                   doel_asset_uuid=doelAsset_uuid)


if __name__ == '__main__':
    bypass = BypassProcessor(
        environment=Environment.PRD
        , input_path_componentenlijst=Path(
            __file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20251218.xlsx'
        , output_excel_path=Path(
            __file__).resolve().parent / 'data' / 'output' / f'lantis_bypass_{datetime.now().strftime(format="%Y-%m-%d")}.xlsx'
    )

    bypass.import_data()

    logging.info('Aanmaken Boomstructuur voor installaties onder Wegkantkast')
    logging.info('Aanmaken installaties')

    bypass.process_installatie(df=bypass.df_assets_wegkantkasten
                               , column_name='Wegkantkast_Object assetId.identificator'
                               , asset_type=AssetType.WEGKANTKAST)

    bypass.process_wegkantkasten()
    bypass.process_wegkantkasten_lsdeel()

    bypass.process_mivlve()
    bypass.process_mivmeetpunten()

    bypass.process_installatie(df=bypass.df_assets_portieken_seinbruggen
                               , column_name='Seinbrug_Object assetId.identificator'
                               , asset_type=AssetType.SEINBRUG)
    bypass.process_seinbruggen()

    bypass.process_galgpaal()

    bypass.process_RSS_groep()
    bypass.process_RSS_borden()

    bypass.process_RVMS_groep()
    bypass.process_RVMS_borden()

    bypass.process_camera()

    logging.info('Boomstructuur van de Hoogspanningscabine')
    logging.info('Aanmaken Boomstructuur voor installaties onder Wegkantkast')
    logging.info('Aanmaken installaties')
    bypass.process_installatie(df=bypass.df_assets_voeding, column_name='HSCabine_Object assetId.identificator',
                               asset_type=AssetType.HSCABINE)

    logging.info('Aanmaken Hoogspannings Cabine')
    bypass.process_voeding_HS_cabine()
    bypass.process_voeding_hoogspanningsdeel()
    bypass.process_voeding_laagspanningsdeel()

    bypass.process_voeding_hoogspanning()

    bypass.process_voeding_DNBHoogspanning()
    bypass.process_voeding_energiemeter_DNB()

    bypass.process_voeding_cabinecontroller()
    bypass.process_voeding_segmentcontroller()

    bypass.process_voeding_wegverlichtingsgroep()
    bypass.process_openbare_verlichting()