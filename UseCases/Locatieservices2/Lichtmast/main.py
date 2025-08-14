import logging
from pathlib import Path
import pandas as pd

from API.Locatieservices2Client import Locatieservices2Client

from API.Enums import AuthType, Environment

from pipeline import enrich_assets

def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info(
        """
        'Analyse van de locatie van de Lichtmasten
        'Afleiden van de locatie op basis van de naam van de Lichtmast, door gebruik te maken van de Locatieservice2 API
        """)

    settings = load_settings()
    ls2 = Locatieservices2Client(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings)
    infile = Path.home() / 'Downloads' / 'Lichtmast' / 'lichtmast_zonder_afgeleide_locatie' / 'DA-2025-46771_export.xlsx'
    outfile = Path.home() / 'Downloads' / 'Lichtmast' / 'lichtmast_zonder_afgeleide_locatie' / 'DA-2025-XXXXX_import_test.xlsx'

    usecols = ['typeURI', 'assetId.identificator', 'naam', 'naampad', 'toestand', 'geometry']
    df = pd.read_excel(infile, sheet_name='Lichtmast', usecols=usecols)
    df["naam"] = df["naam"].apply(str)
    df = enrich_assets(df, ls2, add_osm=False, add_prov=True)
    df.to_excel(outfile, sheet_name='Lichtmast', freeze_panes=[1, 2], index=False)