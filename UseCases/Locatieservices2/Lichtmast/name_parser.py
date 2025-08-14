import re
from utils.locatieservice_helpers import convert_ident8

# LICHTMAST_REGEX = r'[A-Za-z]\d{1,3}[NPMXnpmx]\d+\.?\d*'
LICHTMAST_REGEX = r'[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*'


def is_full_match(name: str) -> bool:
    return bool(re.fullmatch(LICHTMAST_REGEX, name))

def parse_lichtmast_naam(naam: str) -> tuple[str,str,str]:
    base = re.match(r'[A-Za-z]\d{1,3}[NPMXnpmx]\d+\.?\d*', naam)
    if not base:
        raise ValueError(f"invalid mast name {naam!r}")
    txt = base.group()
    m = re.search(r'(?<=[0-9])([MNPX])(?=[0-9])', txt, re.IGNORECASE)
    if not m:
        raise ValueError(f"direction not found in {naam!r}")
    idx = m.start()
    pos = m.group()
    ident8 = convert_ident8(txt[:idx], direction=pos)
    opschrift = f"{round(float(txt[idx+1:]), 1)}"
    return pos, ident8, opschrift

