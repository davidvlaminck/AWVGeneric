import pandas as pd
from pathlib import Path


def load_settings(user: str = 'Dries'):
    if user == 'Dries':
        return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    else:
        raise NotImplementedError(f'user: {user} is not implemented in function call load_settings()')

def read_rsa_report(filepath: Path, usecols: [str] = None) -> pd.DataFrame:
    """Read RSA-report as input into a DataFrame."""
    if not usecols:
        usecols = ["uuid"]
    return pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=usecols)