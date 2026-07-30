import json
from pathlib import Path
from API.eminfra.EMInfraDomain import RelatieEnum

_ASSETRELATIES_PATH = Path(__file__).parent / "assetrelaties.json"

# Query pagination defaults
DEFAULT_PAGE_SIZE = 10
LARGE_PAGE_SIZE = 100
SINGLE_RESULT_PAGE_SIZE = 1

with open(_ASSETRELATIES_PATH, "r", encoding="utf-8") as _f:
    _ASSETRELATIES_DICT: dict[str, list[str]] = json.load(_f)

def get_kenmerktype_and_relatietype_id(relatie: RelatieEnum) -> tuple[str, str]:
    """
    Returns kenmerktype_uuid and relatietype_uuid.

    :param relatie: RelatieEnum
    :return: Tuple of strings kenmerktype_uuid and relatietype_uuid
    """
    try:
        return tuple(_ASSETRELATIES_DICT[relatie.value])
    except KeyError:
        raise KeyError(
            f"No relation mapping found for '{relatie.value}'. "
            f"Ensure the relation is defined in '{_ASSETRELATIES_PATH}'."
        )