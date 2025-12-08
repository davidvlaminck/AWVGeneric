import re

def validate_ean(ean: str) -> bool:
    """
    Valideer een 18 digit Elektrisch aansluitnummer (EAN).

    De som van de even cijfers (start met het eerste cijfer "5").
    De som van de oneven cijfers (exclusief het laatste controle cijfer).

    3 * "som even cijfers" + "som oneven cijfers" + laatste cijfer = veelvoud van 10.

    """
    # Vormvereiste: moet 18 cijfers zijn en starten met "54"
    if not re.fullmatch(r"54\d{16}", ean):
        return False

    digits = [int(x) for x in ean]

    # Opsplitsen: alle cijfers behalve het controlecijfer
    main_digits = digits[:-1]
    check_digit = digits[-1]

    sum_even = 0
    sum_odd = 0

    # Index start vanaf 1, conform Cypher reduce()
    for idx, n in enumerate(main_digits, start=0):
        if idx % 2 == 0:
            sum_even += n
        else:
            sum_odd += n

    # Cypher checksum: (3 * even + odd + check_digit) % 10 == 0
    return (3 * sum_even + sum_odd + check_digit) % 10 == 0