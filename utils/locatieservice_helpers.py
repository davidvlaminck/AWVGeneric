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
    if direction not in ('N', 'P'):
        direction = 'P'

    weg_letter = ident8[:1]
    weg_nummer = ident8[1:].rjust(3, '0')

    if match := re.search(
        string=ident8, pattern='([a-zA-Z])$', flags=re.IGNORECASE
    ):
        ident8_suffix = match[1].lower()
        richting_deel1 = 901 + (ord(ident8_suffix) - ord('a'))  # 'a' → 901, ..., 'z' → 926
    else:
        richting_deel1 = '000'
    richting_deel2 = '1' if direction == 'P' else '2'
    return f'{weg_letter}{weg_nummer}{richting_deel1}{richting_deel2}'