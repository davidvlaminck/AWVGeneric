# to do: implement function that converts a short ident8 (e.g. N8) to

def convert_ident8(ident8: str, direction: str = 'P') -> str:
    """
    Converts a short notation of an ident8 road to a long notation of an ident8 road.
    For example N8 > N0080001
    :param ident8:
    :param direction P (positive) or N (negative) direction
    :return: 8 character number
    """
    if direction not in ('N', 'P'):
        raise ValueError('Parameter direction should be ''P'' (positive) or ''N'' (negative).')
    weg_letter = ident8[:1]
    weg_nummer = ident8[1:].ljust(4, 0)
    if direction == 'P':
        richting = '0001'
    else:
        richting = '0002'
    return f'{weg_letter}{weg_nummer}{richting}'