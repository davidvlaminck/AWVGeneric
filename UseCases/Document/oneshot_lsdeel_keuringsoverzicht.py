"""One-shot script: overzicht per LSDeel van keuringseigenschappen en keuringsverslag-documenten.

Scope notes (OneNote)
- Scope: actieve assets van type LSDeel in AIM.
- Per asset: `datum_laatste_keuring`, `resultaat_keuring`, en of er minstens 1 document
  met categorie `KEURINGSVERSLAG` bestaat.
- Output: JSON + CSV in dezelfde map als dit script.
- Buiten scope: documenten downloaden of gegevens updaten in EMInfra.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from API.Enums import AuthType, Environment
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import DocumentCategorieEnum, EigenschapValueDTO
from utils.query_dto_helpers import build_query_search_assettype

SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncOTLDataToLegacy.json")
LSDEEL_URI = "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel"
OUTPUT_JSON_PATH = Path(__file__).with_name("lsdeel_keuringsoverzicht.json")
OUTPUT_CSV_PATH = Path(__file__).with_name("lsdeel_keuringsoverzicht.csv")

DATUM_KEYS = {"datum_laatste_keuring", "datumlaatstekeuring", "datum laatste keuring"}
RESULTAAT_KEYS = {"resultaat_keuring", "resultaatkeuring", "resultaat keuring"}


def normalize_name(value: str) -> str:
    """Normaliseer eigenschapsnaam voor robuuste vergelijking."""
    return "".join(ch for ch in value.lower() if ch.isalnum() or ch == "_")


def typed_value_to_text(typed_value: dict[str, Any]) -> str | None:
    """Haal de leesbare waarde uit `typedValue` op een defensieve manier."""
    if typed_value is None:
        return None

    if "value" in typed_value:
        value = typed_value.get("value")
        return None if value is None else str(value)

    if "values" in typed_value:
        values = typed_value.get("values")
        if values is None:
            return None
        return ", ".join(str(item) for item in values)

    # Fallback voor onverwachte typedValue-structuren.
    return json.dumps(typed_value, ensure_ascii=True)


def extract_keuring_values(eigenschappen: list[EigenschapValueDTO]) -> tuple[str | None, str | None]:
    """Extraheer datum/resultaat van laatste keuring uit de eigenschapwaarden."""
    datum_laatste_keuring = None
    resultaat_keuring = None

    for eigenschapwaarde in eigenschappen:
        naam = eigenschapwaarde.eigenschap.naam
        naam_normalized = normalize_name(naam)

        if naam_normalized in DATUM_KEYS:
            datum_laatste_keuring = typed_value_to_text(eigenschapwaarde.typedValue)

        if naam_normalized in RESULTAAT_KEYS:
            resultaat_keuring = typed_value_to_text(eigenschapwaarde.typedValue)

    return datum_laatste_keuring, resultaat_keuring


def has_keuringsverslag_document(client: EMInfraClient, asset_uuid: str) -> bool:
    """Controleer of een asset minstens 1 document in categorie KEURINGSVERSLAG heeft."""
    document_generator = client.document_service.get_documents_by_uuid_generator(
        asset_uuid=asset_uuid,
        size=1,
        categorie=[DocumentCategorieEnum.KEURINGSVERSLAG],
    )
    return next(document_generator, None) is not None


def write_outputs(rows: list[dict[str, Any]]) -> None:
    """Schrijf de resultaten als JSON en CSV weg."""
    with OUTPUT_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(rows, json_file, indent=2, ensure_ascii=False)

    fieldnames = [
        "asset_uuid",
        "asset_naam",
        "datum_laatste_keuring",
        "resultaat_keuring",
        "heeft_keuringsverslag",
    ]
    with OUTPUT_CSV_PATH.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Run de one-shot export voor LSDeel-assets."""
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=SETTINGS_PATH)

    lsdeel_assettype = eminfra_client.assettype_service.search_assettype(uri=LSDEEL_URI)
    query_dto = build_query_search_assettype(assettype_uuid=lsdeel_assettype.uuid)

    rows: list[dict[str, Any]] = []
    for index, asset in enumerate(eminfra_client.asset_service.search_assets_generator(query_dto=query_dto), start=1):
        if index % 100 == 0:
            print(f"Verwerkt: {index} LSDeel-assets")

        eigenschapwaarden = eminfra_client.eigenschap_service.get_eigenschappen(asset_uuid=asset.uuid)
        datum_laatste_keuring, resultaat_keuring = extract_keuring_values(eigenschappen=eigenschapwaarden)

        rows.append(
            {
                "asset_uuid": asset.uuid,
                "asset_naam": asset.naam,
                "datum_laatste_keuring": datum_laatste_keuring,
                "resultaat_keuring": resultaat_keuring,
                "heeft_keuringsverslag": has_keuringsverslag_document(
                    client=eminfra_client, asset_uuid=asset.uuid
                ),
            }
        )

    write_outputs(rows=rows)
    print(f"Klaar. Aantal LSDeel-assets: {len(rows)}")
    print(f"JSON: {OUTPUT_JSON_PATH}")
    print(f"CSV:  {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main()

