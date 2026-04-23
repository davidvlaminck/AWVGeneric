# AWVGeneric

A Python SDK for interacting with the **EMInfra** (eMOW Infrastructure) platform — the asset management system used by the Flemish road authority (AWV). AWVGeneric provides a generic, reusable API client layer together with a library of ready-to-run use cases for common infrastructure asset operations.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Authentication Methods](#authentication-methods)
- [Quick Start](#quick-start)
- [Core Services](#core-services)
- [Use Cases](#use-cases)
- [Running Tests](#running-tests)
- [Contributing](#contributing)

---

## Overview

AWVGeneric wraps the EMInfra REST API in a strongly-typed, service-oriented Python library. It supports multiple environments (production, test, development) and multiple authentication strategies (JWT, certificates, cookies), and includes more than 20 domain-specific service classes covering the full asset lifecycle — from creation and geometry through contracts, supervision and documents.

On top of the core library, the `UseCases/` folder contains production-ready scripts for real-world operations such as bulk asset updates, tender-contract linking, data-quality checks and legacy system migrations.

---

## Features

- **Multi-environment support** — `PRD`, `TEI`, `DEV`, `AIM`
- **Multiple authentication methods** — JWT, X.509 certificates, session cookies
- **20+ domain services** — assets, relationships, properties, geometry, documents, contracts, supervision, events, …
- **Memory-efficient pagination** — generator-based streaming for large result sets
- **Rich query builder** — composable `QueryDTO` with expressions, operators and paging modes
- **Excel integration** — read/write Excel reports with asset hyperlinks via `openpyxl` / `pandas`
- **ServiceNow Gateway client** — filter and relay events via SNGateway
- **OneDrive client** — upload and retrieve files via Microsoft Graph
- **Location services** — spatial queries and WKT geometry validation
- **23+ production use cases** — ready-to-run scripts for common operational tasks
- **Custom exceptions** — `AssetsMissingError`, `ObjectAlreadyExistsError`
- **Automatic HTTP retries** — built into `AbstractRequester`

---

## Repository Structure

```
AWVGeneric/
├── API/                          # Core SDK
│   ├── eminfra/                  # EMInfra service layer
│   │   ├── EMInfraClient.py      # Main orchestrator — entry point for all services
│   │   ├── EMInfraDomain.py      # Dataclasses, DTOs and enums
│   │   ├── AssetService.py       # Asset CRUD and search
│   │   ├── BestekService.py      # Tender / contract management
│   │   ├── RelatieService.py     # Asset relationship management
│   │   ├── DocumentService.py    # Document upload and retrieval
│   │   ├── GeometrieService.py   # Spatial / geometry operations
│   │   ├── ToezichterService.py  # Supervision management
│   │   ├── AgentService.py       # Agent / organisation management
│   │   ├── EventService.py       # Asset lifecycle events
│   │   ├── PostitService.py      # Comments and post-its
│   │   └── ...                   # More services
│   ├── AbstractRequester.py      # Base HTTP client with retry logic
│   ├── JWTRequester.py           # JWT authentication
│   ├── CertRequester.py          # Certificate-based authentication
│   ├── CookieRequester.py        # Cookie-based authentication
│   ├── RequesterFactory.py       # Factory that creates the correct requester
│   ├── Enums.py                  # AuthType and Environment enums
│   ├── settings_loader.py        # Load settings from JSON file
│   ├── settings_sample.json      # Example settings file (copy and edit)
│   ├── SNGatewayClient.py        # ServiceNow Gateway client
│   ├── EMSONClient.py            # EM-SON legacy client
│   ├── OneDriveClient.py         # OneDrive / Microsoft Graph client
│   └── Locatieservices2Client.py # Location services v2 client
├── Exceptions/
│   ├── AssetsMissingError.py
│   └── ObjectAlreadyExistsError.py
├── Generic/
│   └── ExcelModifier.py          # Add EMInfra / ELISA hyperlinks to Excel files
├── UnitTests/
│   ├── Domain_tests.py
│   ├── PatternVisualiser_tests.py
│   └── Utils_tests.py
├── UseCases/                     # Production use case scripts
│   ├── main_template.py          # Template for new use cases
│   ├── utils.py                  # Shared helpers (logging, settings, Excel I/O)
│   ├── Asset/                    # Asset operations
│   ├── Bestekkoppeling/          # Tender / contract linking (per project)
│   ├── Toezichter/               # Supervisor management
│   ├── Document/                 # Document management
│   ├── DQ_Voeding/               # Data quality — power supply assets
│   ├── Tunnels/                  # Tunnel-specific operations
│   └── ...                       # More use cases
├── utils/                        # Shared low-level helpers
│   ├── eigenschap_helpers.py
│   ├── query_dto_helpers.py
│   ├── wkt_geometry_helpers.py
│   ├── date_helpers.py
│   └── ...
├── requirements.txt
└── OTLMOW_demo_BIM4Infra.ipynb   # Jupyter notebook demo
```

---

## Prerequisites

- Python 3.9 or higher
- Access credentials for the EMInfra platform (JWT key files **or** X.509 certificates)

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/AWV-AIM-BIM/AWVGeneric.git
cd AWVGeneric

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Configuration

All authentication details are stored in a local `settings.json` file that is **never committed** to source control (it is listed in `.gitignore`).

Copy the sample file and fill in your credentials:

```bash
cp API/settings_sample.json API/settings.json
```

The sample file structure:

```jsonc
{
    "authentication": {
        "JWT": {
            "prd": {
                "client_id": "00000000-0000-0000-0000-000000000000",
                "key_path": "path/to/key_prd_private.json"
            },
            "tei": {
                "client_id": "00000000-0000-0000-0000-000000000000",
                "key_path": "path/to/key_tei_private.json"
            },
            "dev": {
                "client_id": "00000000-0000-0000-0000-000000000000",
                "key_path": "path/to/key_dev_private.json"
            }
        },
        "CERT": {
            "prd": {
                "cert_path": "path/to/cert_prd.crt",
                "key_path":  "path/to/key_prd.key"
            }
        }
    }
}
```

For the **OneDrive / Microsoft Graph** integration, see `API/settings.example.json` for the required Azure App Registration fields.

---

## Authentication Methods

| Method | Class | When to use |
|--------|-------|-------------|
| JWT (RSA key pair) | `JWTRequester` | Service-to-service or automated scripts |
| X.509 Certificate  | `CertRequester` | Certificate-based service accounts |
| Session Cookie     | `CookieRequester` | Interactive / browser-based sessions |

The correct requester is created automatically by `RequesterFactory` based on the `AuthType` you pass to `EMInfraClient`.

---

## Quick Start

```python
from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from UseCases.utils import load_settings_path

# Create a client — uses JWT against the test (TEI) environment
client = EMInfraClient(
    env=Environment.TEI,
    auth_type=AuthType.JWT,
    settings_path=load_settings_path()
)

# Fetch a single asset by UUID
asset = client.asset_service.get_asset_by_uuid("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
print(asset.naam, asset.actief)

# Stream all assets of a given type (memory-efficient generator)
from API.eminfra.EMInfraDomain import QueryDTO, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum

query = QueryDTO(
    size=100,
    selection=SelectionDTO(
        expressions=[
            ExpressionDTO(terms=[
                TermDTO(property="type", operator=OperatorEnum.EQ, value="<asset-type-uuid>"),
                TermDTO(property="actief",  operator=OperatorEnum.EQ, value=True),
            ])
        ]
    )
)

for asset in client.asset_service.search_assets_generator(query):
    print(asset.uuid, asset.naam)
```

---

## Core Services

All services are accessible as attributes of `EMInfraClient`:

| Attribute | Service class | Responsibility |
|-----------|--------------|----------------|
| `asset_service` | `AssetService` | Asset CRUD, search, state management |
| `relatie_service` | `RelatieService` | Asset-to-asset relationships |
| `bestek_service` | `BestekService` | Tender / contract management |
| `document_service` | `DocumentService` | Document upload and linking |
| `geometrie_service` | `GeometrieService` | Spatial data and WKT geometry |
| `toezichter_service` | `ToezichterService` | Supervisor / inspection management |
| `agent_service` | `AgentService` | Organisations and people |
| `eigenschap_service` | `EigenschapService` | Custom asset properties |
| `kenmerk_service` | `KenmerkService` | Asset markers / characteristics |
| `locatie_service` | `LocatieService` | Location-based queries |
| `event_service` | `EventService` | Asset lifecycle events |
| `postit_service` | `PostitService` | Comments and post-its |
| `graph_service` | `GraphService` | Graph-based asset queries |
| `beheerobject_service` | `BeheerobjectService` | Management object operations |
| `schadebeheerder_service` | `SchadebeheerderService` | Damage manager operations |

---

## Use Cases

The `UseCases/` directory contains ready-to-run scripts for real-world operations. Each use case lives in its own subdirectory with a `main.py` entry point.

**Running a use case:**

```bash
# Example: get assets by name
python UseCases/Asset/GetAssetsByName/main.py

# Example: update supervisors in bulk
python UseCases/Toezichter/main.py
```

**Creating a new use case:**

Copy `UseCases/main_template.py` to a new subdirectory and adjust the client configuration, query and output logic.

```
UseCases/
├── Asset/
│   ├── ChangeName/
│   ├── GetAssetsByName/
│   ├── GetAssetsByType/
│   └── GetAssets_Toezichter/
├── Bestekkoppeling/           # Per-project tender linking scripts
│   ├── Kennedytunnel/
│   ├── Oostertunnel/
│   └── ...
├── Toezichter/
├── Document/
├── DQ_Voeding/
├── Tunnels/
└── ...
```

---

## Running Tests

```bash
# Run all unit tests
python -m pytest UnitTests/

# Run a specific test file
python -m pytest UnitTests/Domain_tests.py -v
```

> **Note:** Integration tests that call the live EMInfra API require a valid `API/settings.json` with credentials for the target environment.

---

## Contributing

1. Create a feature branch from `main`.
2. Follow the existing naming conventions:
   - Service classes: `*Service`
   - Data-transfer objects: `*DTO`
   - Enumerations: `*Enum`
   - Test files: `*_tests.py`
3. Add or update unit tests in `UnitTests/` for any new logic.
4. Never commit `API/settings.json` or any private key / certificate files.
5. Open a pull request with a clear description of the change.
