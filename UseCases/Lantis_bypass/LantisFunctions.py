from API.eminfra.EMInfraDomain import AssetDTOToestand, RelatieEnum


def map_status(nieuwe_status: str) -> AssetDTOToestand:
    """
    Map een string naar de enumeration klasse AssetDTOToestand.
    """
    mapping = {
        'geannuleerd': AssetDTOToestand.GEANNULEERD,
        'gepland': AssetDTOToestand.GEPLAND,
        'in-gebruik': AssetDTOToestand.IN_GEBRUIK,
        'in-ontwerp': AssetDTOToestand.IN_ONTWERP,
        'in-opbouw': AssetDTOToestand.IN_OPBOUW,
        'overgedragen': AssetDTOToestand.OVERGEDRAGEN,
        'uit-gebruik': AssetDTOToestand.UIT_GEBRUIK,
        'verwijderd': AssetDTOToestand.VERWIJDERD,
    }

    try:
        return mapping[nieuwe_status]
    except KeyError as e:
        allowed = ", ".join(mapping.keys())
        raise ValueError(
            f"Toestand '{nieuwe_status}' is niet gekend. "
            f"Gebruik een van volgende waarden: {allowed}"
        ) from e


def map_relatie(relatie_uri: str) -> RelatieEnum:
    mapping = {
        'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging': RelatieEnum.BEVESTIGING,
        'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt': RelatieEnum.VOEDT,
        'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing': RelatieEnum.STURING,
        'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij': RelatieEnum.HOORTBIJ,
    }
    try:
        return mapping[relatie_uri]
    except KeyError as e:
        allowed = ", ".join(mapping.keys())
        raise ValueError(
            f"Relatie_uri '{relatie_uri}' is niet gekend. "
            f"Gebruik een van volgende waarden: {allowed}"
        ) from e