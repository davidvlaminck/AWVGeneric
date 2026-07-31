# API Folder — Detailed Suggestions (Updated 2026-07-31)

Based on a re-execution of the line-by-line review of the `API/` folder.
Each item is annotated with **Status**: ✅ FIXED, ❌ NOT FIXED, or ⚠️ PARTIALLY FIXED.

---

## 1. Critical Bugs
**Status:** ✅ *Done*

---

## 2. Design Pattern Violations

### 2.1 Dangerous module-level monkey-patch
`eminfra/EMInfraDomain.py:7-25` replaces `dataclasses._asdict_inner` globally.  
**Status:** ❌ NOT FIXED  
**Suggestion:** Remove both monkey-patches in `EMInfraDomain.py` and `Locatieservices2Domain.py`. The `__dict_factory_override__` method on `BaseDataclass` already provides the custom serialization path without needing to patch `dataclasses` internals.

### 2.2 Duplicated `_by_uuid` / by-object wrapper pattern
Every service duplicates UUID-based and DTO-based methods.  
**Status:** ❌ NOT FIXED  
**Suggestion:** Introduce a filter/mixin or decorator to reduce the ~2x method proliferation.

### 2.3 Duplicated pagination loops
Copy-pasted `while True` pagination across `AssetService`, `ToezichterService`, `AssettypeService`, `EventService`, `KenmerkService`, `BeheerobjectService`, `PostitService`, `DocumentService`, `AgentService`.  
**Status:** ❌ NOT FIXED  
**Suggestion:** Extract a shared `PaginatedIterator` or internal generator.

### 2.4 Duplicated error-handling template
`raise ProcessLookupError(response.content.decode("utf-8"))` + `logging.error(response)` repeated.  
**Status:** ❌ NOT FIXED  
**New observation:** `EMSONClient.py` uses `print(response)` instead of `logging.error(response)`.  
**Suggestion:** Add a single `_raise_for_status(response, method)` helper to `AbstractRequester`.

---

## 3. Error Handling Inconsistencies

### 3.1 Wrong exception type
`ProcessLookupError` used everywhere instead of a domain-specific exception.  
**Status:** ❌ NOT FIXED  
**New observation:** `DocumentService.remove_document` (line 161) raises `ValueError(...)` instead of `ProcessLookupError`.  
**Suggestion:** Create `EMInfraAPIError(RuntimeError)`.

### 3.2 Inconsistent logging
Some methods log before raising, others don't; `EMSONClient` uses `print()`.  
**Status:** ❌ NOT FIXED  
**Suggestion:** Delegate to a shared error helper.

### 3.3 Inconsistent status codes
No project-wide convention (GET→200, PUT→202, POST→200/201).  
**Status:** ❌ NOT FIXED  
**Suggestion:** Document expected status per operation type.

---

## 4. Naming & Conventions

### 4.1 Dutch/English mixing
Docstrings/comments predominantly Dutch; methods/classes English.  
**Status:** ❌ NOT FIXED

### 4.2 Shadowing built-ins
- `AssetService.py:396` uses parameter `filter_dict` — ✅ **FIXED** (renamed from `filter` to `filter_dict`).
- `ToezichterService.py:97` — **✅ FIXED** (renamed to `toezichtgroep_type`).

### 4.3 camelCase field names in dataclasses
**Status:** ❌ NOT FIXED — `EMInfraDomain.py` still uses `totalCount`, `createdOn`, `modifiedOn`, `actiefInterval`, etc.

### 4.4 `from_` inconsistency
`DTOList._from` vs `QueryDTO.from_`.  
**Status:** ⚠️ PARTIALLY FIXED — `QueryDTO` uses `from_` consistently; `DTOList` still uses `_from` (line 212).

### 4.5 Static-like methods called on instances
- `AssetService.py:264` — `BeheerobjectService.get_beheerobject(beheerobject_uuid=parent_uuid)`
  - **Status:** 🆕 **NEW — RUNTIME BUG.** `BeheerobjectService` IS imported (line 7), but `get_beheerobject` is an instance method. Calling it on the class without `self` raises `TypeError`.
  - **Suggestion:** Use `BeheerobjectService(self.requester).get_beheerobject(beheerobject_uuid=parent_uuid)`.
- `RelatieService.py:160` — `AssetService.get_asset_by_uuid(self, asset_uuid=...)`
  - **Status:** ❌ NOT FIXED — Passes `self` (a `RelatieService`) as `self` to `AssetService.get_asset_by_uuid`. Works via duck-typing but is fragile.
- `SchadebeheerderService.py:16` — `KenmerkService.get(self, asset_uuid, self.SCHADEBEHEERDER_UUID)`
  - **Status:** ❌ NOT FIXED — Same duck-typing hack.

---

## 5. Hardcoded Values

### 5.1 Hardcoded UUIDs as class constants
**Status:** ❌ NOT FIXED

### 5.2 Hardcoded dictionary in `Generic.py`
**Status:** ✅ FIXED — Now loads `_ASSETRELATIES_DICT` from `assetrelaties.json` at module level (lines 12-13).

### 5.3 Hardcoded query sizes
**Status:** ⚠️ PARTIALLY FIXED — `Generic.py` defines `DEFAULT_PAGE_SIZE=10`, `LARGE_PAGE_SIZE=100`, `SINGLE_RESULT_PAGE_SIZE=1`. Some services use these; others still hardcode (e.g., `AgentService.py:56` `query_dto.size = 100`, `PostitService.py:51` `query_dto.size = 100`).

---

## 6. Type Hint Issues

### 6.1 Missing generic parameters
**Status:** ⚠️ PARTIALLY FIXED
- ✅ Many `list[T]` fields now have parameters (e.g., `EMInfraDomain.py:207` `data: list[AssettypeDTO]`).
- ❌ Still bare `list`: `EMInfraDomain.py:217` `data: list` (in `DTOList`), should be `list[AssettypeDTO]`.
- ❌ Still bare `dict`: `EMInfraDomain.py:224` `terms: list[dict] | list[TermDTO]`, `EMInfraDomain.py:233-234` `expressions: list[dict] | list[ExpressionDTO]`, `settings: dict | None`; also `EMInfraDomain.py:354` `data: dict`, `EMInfraDomain.py:363` `type: dict`, `EMInfraDomain.py:365-368` `locatie/relatie/elektriciteitsAansluitingRef: dict`; `EMInfraDomain.py:438-441` `actiefInterval/contactFiche/afdeling/districtDiensten: dict`; `EMInfraDomain.py:579` `type: dict | None`; `EMInfraDomain.py:689` `type: dict`; `EMInfraDomain.py:693` `authorizationMetadata: dict | None`; `EMInfraDomain.py:747` `geldigheid: dict | None`; `EMInfraDomain.py:774-778` `types/bestekRef/bestekKoppelingen/toezichter/toezichtGroep: dict`; `EMInfraDomain.py:811` `type: dict`; `AssetService.py:396` `filter_dict: dict`; `AssetService.py:402` `expansions_fields: list[str] = None` still uses bare `dict`.
- ❌ Still `Generator[T]` with 1 param: 31 occurrences across 11 files (e.g., `AgentService.py:11`, `AssetService.py:133`, etc.).

### 6.2 Non-None defaults typed as optional
**Status:** ⚠️ PARTIALLY FIXED
- ❌ `GraphService.py:67` `relatietypes: list = None` — still missing `| None` (line 36 was fixed but line 67 was not).
- ❌ `AssetService.py:27` `_update_asset`: `naam: str = None`, `commentaar: str = None` — still missing `| None`.
- ❌ `AssetService.py:57-58` `update_asset_by_uuid`: `naam: str = None`, `toestand: AssetDTOToestand = None` — still missing `| None`.
- ❌ `BestekService.py:161` `adjust_date_bestekkoppeling`: `start_datetime: datetime = None` — still missing `| None`.
- ❌ `LocatieService.py:39` `wkt_geometry: str = None`, line 68 `doel_asset: AssetDTO = None` — still missing `| None`.
- ⚠️ `ToezichterService.py:129` `bron: Optional[str] | None` — redundant `Optional` + `| None`.

### 6.3 Incorrect type annotations
- ✅ `AssetService.py:401` `filter_dict: dict = {}` — **FIXED** (was `'{}'` string, now `{}` dict).
- ✅ `BestekService.py:102` `bestekkoppelingen: list[BestekKoppeling]` — **FIXED** (was `[BestekKoppeling]`).
- ❌ `KenmerkService.py:29` -> `[AssetTypeKenmerkTypeDTO]` — should be `list[AssetTypeKenmerkTypeDTO]`.
- ❌ `RelatieService.py:66-67` -> `[AssetRelatieDTO]` — should be `list[AssetRelatieDTO]`.
- ❌ `RelatieService.py:110` -> `[AssetDTO]` — should be `list[AssetDTO]`.
- ❌ `EMSONClient.py:53,85` -> `[dict]` — should be `list[dict]`.
- ❌ `AssetService.py:402` `expansions_fields: [str] = None` — should be `list[str] | None = None`.

---

## 7. Mutable / Problematic Default Arguments

### 7.1 `datetime.now()` evaluated at definition time
**Status:** ⚠️ PARTIALLY FIXED
- ✅ `BestekService.add_bestekkoppeling_by_uuid` — now uses `start_datetime: datetime | None = None`.
- ❌ `BestekService.replace_bestekkoppeling` (line 345) — **still has** `start_datetime: datetime = datetime.now()`.
- ❌ Other methods in BestekService may still have the same issue.

### 7.2 `from_dict` mutates input dict
- ✅ `EMInfraDomain.py` — **FIXED** (uses `data = dict_.copy()` on line 139).
- ❌ `Locatieservices2Domain.py:56-61` — **still mutates input dict** in place.

---

## 8. Requesters

### 8.1 URL mutation in constructors
`EMSONClient.py:21`, `SNGatewayClient.py:11`, `Locatieservices2Client.py:14`, `FSClient.py:13`, `EMInfraClient.py:29` all append to `self.requester.first_part_url`.  
**Status:** ❌ NOT FIXED

### 8.2 `JWTRequester` checks module presence via `sys.modules`
**Status:** ✅ FIXED — Now uses `try: import cryptography; except ImportError: raise ModuleNotFoundError(...)`. `import sys` removed.

### 8.3 Header mutation duplication
**Status:** ⚠️ PARTIALLY FIXED — ✅ `_apply_default_headers` extracted to `AbstractRequester.py:138-153`. `CookieRequester` and `CertRequester` now inherit it (no duplicate methods).  
- 🆕 **NEW — BUG in `JWTRequester.py:66-83`:** After calling `self._apply_default_headers(kwargs)` (which sets `accept`), the method **re-applies** the same accept logic (lines 73-80), producing `"application/json, application/json"` instead of `"application/json"`. The redundant block (lines 73-80) must be removed.

### 8.4 `JWTRequester.generate_authentication_token` uses `random.choice`
**Status:** ✅ FIXED — Now uses `secrets.token_urlsafe(20)` (line 82). `import secrets` added; `import string` and `from random import choice` removed.

### 8.5 `OneDriveClient` does not use `AbstractRequester`
**Status:** ❌ NOT FIXED — Still uses raw `requests.get/post/put`.

---

## 9. Other Issues

### 9.1 `BaseDataclass.asdict` shadowing
- ✅ `EMInfraDomain.py` — **FIXED** (renamed to `to_dict`).
- ❌ `Locatieservices2Domain.py:45` — **NOT FIXED** — still has `def asdict(self)` and calls `self.asdict()` in `json()` (lines 52-53) and `__str__` (line 83).

### 9.2 Dead code
- ✅ `EMInfraDomain.py` commented-out `__post_init__` block — **FIXED** (removed).
- ✅ `GraphService.py` `AssetDTO` import — **FIXED** (it IS used on line 67).

### 9.3 Missing `settings_path` validation
**Status:** ⚠️ PARTIALLY FIXED — `RequesterFactory.py:30-35` now uses `try/except`, but with a **bare `except:`** (line 34) which is too broad.  
**Suggestion:** Use `Path(settings_path).exists()` guard + explicit `FileNotFoundError` message. Also fix `cookie: str = None` → `cookie: str | None = None` and `settings_path: Path = None` → `... | None`.

### 9.4 `EMSONClient` `Query`/`EMSONQuery` duplicates `QueryDTO`
**Status:** ⚠️ PARTIALLY FIXED — Class renamed from `Query` to `EMSONQuery`, but still a separate class with a different shape (`filters` vs `selection`, `fromCursor` only).  
**Suggestion:** Reuse `QueryDTO` if possible, or document the shape difference explicitly.

### 9.5 `FSClient.download_layer_to_records` confusing generator
**Status:** ❌ NOT FIXED — Still uses `yield from` inside a loop with `chunk_rest` reassignment (lines 46-48).

---

## 10. Summary Table (Updated)

| Priority | Count | Area | Status |
|----------|-------|------|--------|
| P0 | 4 | Bugs (infinite recursion, wrong variable, wrong type hints) | ✅ All done |
| P1 | ~6 | Design duplication (pagination, error handling, by_uuid wrappers) | ❌ 6+ items open |
| P2 | 8+ | Hardcoded values, error types, type hint fixes | ⚠️ ~4 fixed, 8+ remaining |
| P3 | ~5 | Naming consistency, dead code, monkey-patch | ⚠️ ~2 fixed, 3 remaining |
| P4 | 4 | Requester improvements | ⚠️ 3 fixed, 1 remaining |
| **NEW** | 3 | New issues discovered during re-review | 🆕 3 new critical issues |

**Recommended refactoring order:**
1. ✅ Fix the 4 critical bugs (done)
2. Replace `ProcessLookupError` with a proper exception family
3. Extract shared pagination and error-handling helpers
4. Clean up hardcoded UUIDs / sizes
5. Standardize naming conventions (snake_case fields)
6. Remove the module-level monkey-patch and dead code
7. Fix the new runtime bug in `AssetService.py:264` (missing `self`)
8. Fix the `accept` header double-append bug in `JWTRequester.py:66-83`
9. Fix the `Locatieservices2Domain.py` `asdict` shadowing and `from_dict` input mutation

---

## 11. Additional Issues — Deep Review

### 11.1 Architectural / Coupling Issues
**Status:** ❌ NOT FIXED
- `SchadebeheerderService.py:16` calls `KenmerkService.get(self, ...)` — passing `self` as `self` to a different class's instance method.
- `AssetService.py:264` calls `BeheerobjectService.get_beheerobject(beheerobject_uuid=...)` — **🆕 NEW: RUNTIME TypeError** — no `self` passed.
- `RelatieService.py:160` calls `AssetService.get_asset_by_uuid(self, ...)` — duck-typing hack.

### 11.2 State Mutation & Side Effects
**Status:** ⚠️ PARTIALLY FIXED
- `EMInfraDomain.py` `from_dict` — ✅ FIXED (defensive copy).
- `Locatieservices2Domain.py` `from_dict` — ❌ NOT FIXED (mutates input).
- Generators mutating `QueryDTO.from_` — ❌ NOT FIXED (many occurrences across services).

### 11.3 Inconsistent HTTP Status Code Expectations
**Status:** ❌ NOT FIXED — No uniform policy.

### 11.4 Response Handling Inconsistencies
**Status:** ❌ NOT FIXED — Unsafe `['data']` indexing, `json.loads(response.content)` in `DocumentService.py:30`, inconsistent `remove_*` return types.

### 11.5 Security Issues
**Status:** ❌ NOT FIXED — Path traversal in `DocumentService.download_document`, `None` sent in JSON bodies.

### 11.6 Performance Concerns
**Status:** ⚠️ PARTIALLY FIXED
- `BeheerobjectService.create_beheerobject` — ❌ NOT FIXED (fetches types on every call).
- `PostitService.edit_postit` — ❌ NOT FIXED (always fetches existing postit).

### 11.7 Testability Concerns
**Status:** ❌ NOT FIXED — No mapper/response_handler abstraction; generators mutate input DTOs.

### 11.8 Import and Type Issues
**Status:** ⚠️ PARTIALLY FIXED
- `Generic.py:103` `-> (str, str)` — ✅ FIXED (now `-> tuple[str, str]`).
- `EigenschapService.py:6` — ✅ `KenmerkTypeEnum` and `AssetDTO` ARE used. ❌ `KenmerkType` is **unused** — remove it.
- `wkt_validator.py:19` — ❌ NOT FIXED — still catches `(ShapelyError, Exception)`.
- 🆕 **NEW:** `BestekService.py:3` imports `NULL` from `asyncio.windows_events` — unused.
- 🆕 **NEW:** `BestekService.py:2` and `GeometrieService.py:2` import `logging` — unused.
- 🆕 **NEW:** `FSClient.py:2` imports `Iterator` from `typing` — unused.

### 11.9 Docstring and Naming Issues
**Status:** ⚠️ PARTIALLY FIXED
- `AssetService.py:27` parameter `is_actief` (was `actief` shadowed built-in) — ✅ FIXED.
- `AssetService.py:127` `asset_uuid = self.get_asset_by_uuid(...)` overwrites UUID variable with DTO — 🆕 **NEW:** misleading variable name.
- Docstring `:type size: str` in `DocumentService` — ❌ NOT FIXED.
- Missing docstrings — ❌ NOT FIXED.

### 11.10 Python Best-Practice Violations
**Status:** ⚠️ PARTIALLY FIXED
- `LocatieService.py:38-39` `doel_asset_uuid: str | None = None` — ✅ FIXED (was `AssetDTO`).
- `ToezichterService.py:129` `bron: Optional[str] | None = None` — ⚠️ redundant `Optional` + `| None`.
- `BestekService.py:272-274` `add_bestekkoppeling` — ✅ FIXED (`AssetDTO` without `= None`).
- Unnecessary f-strings — ❌ NOT FIXED (many occurrences).
- `PostitService.edit_postit` truthiness bug — ❌ NOT FIXED (still uses `commentaar if commentaar else actual_commentaar` on line 131).
- `GeometrieService.update_geometrie` — ❌ NOT FIXED (still passes `asset` instead of `asset.uuid`).

### 11.11 `wkt_validator.py` Catching Bare `Exception`
**Status:** ❌ NOT FIXED

### 11.12 `EMSONClient.Query` Duplicates `QueryDTO`
**Status:** ⚠️ PARTIALLY FIXED (renamed to `EMSONQuery`, but still a separate class).

### 11.13 `AbstractRequester` URL Mutation
**Status:** ❌ NOT FIXED — All client constructors still mutate `self.requester.first_part_url`.

### 11.14 `RequesterFactory.create_requester` Path Validation
**Status:** ⚠️ PARTIALLY FIXED — Uses try/except but with bare `except:`.

---

## 12. New Issues Found During Re-review (Not in Original Suggestions)

### 12.1 Runtime `TypeError` in `AssetService.search_parent_asset_by_uuid`
- **File:** `AssetService.py:264`
- **Code:** `parent_asset = BeheerobjectService.get_beheerobject(beheerobject_uuid=parent_uuid)`
- **Issue:** `get_beheerobject` is an instance method; calling without `self` raises `TypeError`.
- **Suggestion:** `BeheerobjectService(self.requester).get_beheerobject(beheerobject_uuid=parent_uuid)`

### 12.2 Redundant `accept` header double-append in `JWTRequester`
- **File:** `JWTRequester.py:66-83`
- **Issue:** `_apply_default_headers` sets `accept`, then the method re-applies the same logic, yielding `"application/json, application/json"`.
- **Suggestion:** Remove lines 73-80; keep only the `authorization` header injection.

### 12.3 Misaligned variable name in `AssetService.deactiveer_asset_by_uuid`
- **File:** `AssetService.py:127`
- **Code:** `asset_uuid = self.get_asset_by_uuid(asset_uuid=asset_uuid)` — reassigns a UUID variable to an `AssetDTO`.
- **Suggestion:** Rename to `asset = self.get_asset_by_uuid(...)`.