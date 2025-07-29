import logging
import re

from API.Enums import AuthType, Environment
from pathlib import Path

from API.Locatieservices2Client import Locatieservices2Client


def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def is_full_match(name: str, pattern: str = '[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*') -> bool:
    """
    Returns True if the entire string matches the given regex pattern.

    :type name: str
    :param name: The input string to test.
    :type pattern: str
    :param pattern: The regular expression pattern.
    :return: Boolean indicating if full match occurred.
    """
    return re.fullmatch(pattern, name) is not None


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Test Locatieservices2 (LS2)')
    settings_path = load_settings()
    ls2_client = Locatieservices2Client(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    logging.debug('Debug functie zoek_puntlocatie_via_xy()')
    x = 158190.21
    y = 164766.15
    wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_xy(x=x, y=y)
    logging.debug(f'Gevonden Puntlocatie: {wegsegment_puntlocatie}')

    logging.debug('Debug functie zoek_puntlocatie_via_wegsegment()')
    ident8='N0080001'
    opschrift='4.6'
    afstand='1.5'
    wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_wegsegment(ident8=ident8, opschrift=opschrift, afstand=afstand)
    logging.debug(f'Gevonden Puntlocatie: {wegsegment_puntlocatie}')

    # Import Excel-file as pandas dataframe

    # Todo functie toevoegen om input te parsen naar ident8, richting en opschrift
    # Start met een regex-validatie
    # omit .P op het einde
    # Splits op basis van de karakters ['P', 'N', 'X', 'M'].
    # Het eerste deel is de ident8 en het tweede deel het opschrift

    # Todo functie toevoegen om een bepaald wegnummer te converteren naar de juiste syntax van ident8
    lichtmast_naam = 'A18N27.45.P'
    is_full_match(name=lichtmast_naam)
    logging.debug('Debug functie name_validation()')

    logging.info('Extraheer het eerste deel van een string')
    lichtmast_naam_basis = re.match(pattern='[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*', string=lichtmast_naam)[0]

    logging.info('Extraheer de aanduiding van de richting (NMPX), alsook de indexpositie in de string')
    match = re.search(pattern=r"(?<=[0-9])([MNPX])(?=[0-9])", string=lichtmast_naam_basis, flags=re.IGNORECASE)
    index = match.start()
    positie_rijweg = match.group()

    ident8_raw = lichtmast_naam_basis[:index]
    opschrift = lichtmast_naam_basis[index:]