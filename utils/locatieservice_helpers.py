import re


def convert_ident8(ident8: str, direction: str = 'P') -> str:
    """
    Converts a short notation of an ident8 road to a long notation of an ident8 road.
    For example: N8 > N0080001
    If the road number is followed by a  letter [a-z], the direction starts with 901, etc...
    a -> 901, b -> 902, etc...

    :param ident8:
    :param direction P (positive) or N (negative) direction
    :return: 8 character number
    """
    if not (match_weg_letter := re.search(string=ident8, pattern=r'^([a-zA-Z])')):
        raise ValueError(f'De wegletter kon niet worden achterhaald uit ident8: {ident8}')
    weg_letter = match_weg_letter[1]

    if not (match_weg_nummer := re.search(string=ident8, pattern=r'(\d+)')):
        raise ValueError(f'Het wegnummer kon niet worden achterhaald uit ident8: {ident8}')
    weg_nummer = match_weg_nummer[1]
    weg_nummer = weg_nummer.rjust(3, '0')

    if match := re.search(string=ident8, pattern='([a-zA-Z])$', flags=re.IGNORECASE):
        ident8_suffix = match[1].lower()
        richting_deel1 = 901 + (ord(ident8_suffix) - ord('a'))  # 'a' → 901, ..., 'z' → 926
    else:
        richting_deel1 = '000'
    richting_deel2 = '2' if direction == 'N' else '1'

    return f'{weg_letter}{weg_nummer}{richting_deel1}{richting_deel2}'