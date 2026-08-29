# vr-douchong-modularization-assessment - Work Plan

## TL;DR (For humans)
<!-- Fill this LAST, after the detailed plan below is written, so it summarizes the REAL plan. -->
<!-- Plain English for a non-engineer: NO file paths, NO todo numbers, NO wave/agent/tool names. -->

**What you'll get:** A staged, behavior-preserving decomposition of the current Python service into six maintainable areas, with database/API/JSON contracts explicitly frozen and a compatibility path for the existing launcher and archive CLI.

**Why this approach:** Extract boundaries in dependency order, starting with characterization evidence and persistence contracts, then move event/runtime orchestration and API transport. This reduces the risk caused by shared globals and import-time initialization.

**What it will NOT do:** It will not change tables, fields, JSON keys, endpoints, formulas, timing, queue semantics, or the bundled protocol client. It will not add a frontend or unrelated dependencies.

**Effort:** Medium
**Risk:** High - the current runtime is a 3,712-line monolith with import-time side effects and shared mutable state, and no existing tests.
**Decisions to sanity-check:** Keep `gift.py` as a compatibility launcher; keep `blivedm` untouched initially; preserve current commit and scheduler semantics.

Your next move: approve the extraction plan before any product-code edits. This assessment itself has not edited product code.

---

> TL;DR (machine): Medium/high-risk behavior-preserving Python modularization plan covering six areas, frozen DB/API/JSON contracts, characterization verification, and compatibility launchers.

## Scope
### Must have
- Preserve all current runtime workflows, API routes/response shapes, room JSON keys, database table/column/index names, archive naming and transaction granularity.
- Establish one-way dependencies: transport/API -> application services -> repositories/integrations; domain/persistence must not import FastAPI or websocket clients.
- Keep `gift.py` executable and keep `migrate_sc_archive.py` working after every extraction wave.
- Add characterization checks before moving behavior and run import/compile/API/archive checks after each wave.
### Must NOT have (guardrails, anti-slop, scope boundaries)
- Do not redesign business logic, schema, external payload contracts, timing, retry, queue, or archive semantics.
- Do not split bundled `blivedm` protocol/model files in the first pass.
- Do not remove unreferenced data files or normalize unrelated dirty-worktree changes.

## Verification strategy
> Zero human intervention - all verification is agent-executed.
- Test decision: characterization-first/TDD-style checks with pytest plus FastAPI client and a staging MySQL database; no live Bilibili calls in automated tests. Capture a controlled schema baseline before any bootstrap DDL.
- Evidence: `.omo/evidence/` plus before/after JSON and schema snapshots captured by the worker.

## Execution strategy
### Parallel execution waves
> Target 5-8 todos per wave. Fewer than 3 (except the final) means you under-split.

- Wave 1: contract inventory and characterization checks.
- Wave 2: persistence/config extraction, independently verifiable.
- Wave 3: room runtime, Bilibili gateway, ingestion and monitoring extraction.
- Wave 4: API/reporting, bootstrap/launcher and archive CLI compatibility.
- Final: full contract, import, staging-DB and surface verification.

### Dependency matrix
| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |
| 1 | none | 2 | none |
| 2 | 1 | 3 | none |
| 3 | 2 | 4 | none |
| 4 | 3 | 5 | none |
| 5 | 3, 4 | 6 | none |
| 6 | 5 | 7 | none |
| 7 | 6 | final wave | none |

## Todos
> Implementation + Test = ONE todo. Never separate.
<!-- APPEND TASK BATCHES BELOW THIS LINE WITH edit/apply_patch - never rewrite the headers above. -->
- [x] 1. Lock current behavior and contracts before extraction
  What to do / Must NOT do: Record all seven route payloads/statuses, including `/gift` versus `/gift/by_month` key asymmetry, room-config round trips, import-time behavior, model/table metadata, archive suffixes, CLI three-function scope and the undocumented environment variable. Do not use live Bilibili endpoints.
  Parallelization: Wave 1 | Blocked by: none | Blocks: 2 | References (executor has NO interview context - be exhaustive): `gift.py:104, 108-115, 118-130, 226-584, 619-1333, 1340-1494, 3151-3712`; `rooms.json:1-122`; `api.md`; `migrate_sc_archive.py:11-42`; `.env.example:1-29`.
  Acceptance criteria (agent-executable): characterization checks pass on the pre-extraction code and capture exact keys/statuses for all seven routes, schema/table/column/index snapshots, room JSON keys and archive CLI invocation set. Capture the schema baseline after controlled bootstrap.
  QA scenarios (name the exact tool + invocation): happy: `pytest -q` against fixtures/staging DB; failure: invalid `room_id`, invalid `month`, missing API key and current-month archive all retain existing responses/skip behavior. Evidence `.omo/evidence/task-1-vr-douchong-modularization-assessment.json`.
  Commit: Y | test(contract): capture current API and persistence behavior
- [x] 2. Extract configuration, DB bootstrap, models and repositories
  What to do / Must NOT do: Move env/config loading, engine/session setup, ORM models and aggregate repositories while preserving every table/column/index and current commit/rollback behavior. Convert `create_all`/`ensure_runtime_schema` into explicit bootstrap called by the launcher and controlled CLI/test setup; do not run DDL on plain model import. Keep compatibility imports in `gift.py`; do not introduce schema migrations or rename fields.
  Parallelization: Wave 2 | Blocked by: 1 | Blocks: 3 | References: `gift.py:48-105, 619-1333, 1470-1494`; `.env.example:1-29`; `requirements.txt:1-7`.
  Acceptance criteria (agent-executable): imported models expose identical SQLAlchemy metadata; a plain import performs no DDL; direct `python gift.py` bootstrap produces an empty schema diff against the controlled baseline; all repository characterization checks pass. Include `ATTENTION_DAILY_ROOM_SLEEP_SECONDS` in the captured config contract without changing `.env.example` in this refactor.
  QA scenarios: happy: import model modules and perform one read/write per aggregate, then launch through `python gift.py`; failure: DB error rolls back and closes sessions as before, while a plain import does not mutate schema. Evidence `.omo/evidence/task-2-vr-douchong-modularization-assessment.json`.
  Commit: Y | refactor(db): extract persistence boundaries without schema changes
- [x] 3. Extract room lifecycle and runtime state
  What to do / Must NOT do: First establish one `runtime_state`/`queues` owner for all room/session caches and asyncio queues. Then move room JSON management, room add/delete, client registry and session lifecycle services behind explicit boundaries. Preserve lock usage, queue tuple shapes, grace period and cleanup order; do not split consumers in parallel with this ownership move.
  Parallelization: Wave 3 | Blocked by: 2 | Blocks: 4 | References: `gift.py:1335-1412, 1523-1629, 1867-2123`; `gift.py:1873-1896`.
  Acceptance criteria (agent-executable): there is exactly one runtime owner for each queue/cache; add/delete room tests produce key-compatible room config and identical success/error results; session lifecycle state transitions match before/after fixtures.
  QA scenarios: happy: add then delete a fixture room without a live client; failure: duplicate/missing room and absent session/client leave the same response and cleanup state. Evidence `.omo/evidence/task-3-vr-douchong-modularization-assessment.json`.
  Commit: Y | refactor(runtime): isolate room lifecycle and state
- [x] 4. Extract Bilibili gateway, event ingestion and monitoring jobs
  What to do / Must NOT do: Keep `blivedm/` transport/protocol/model files unchanged; move Bilibili HTTP calls, `MyHandler`, `COMMON_NOTICE_GIFT_COIN_MAP`, event-to-service mapping, workers, queues, reconnect, ticket, status and concurrency schedulers. Preserve API parameter mapping, event formulas, retry/sleep intervals and task ordering. Archive ownership stays with Todo 5.
  Parallelization: Wave 3 | Blocked by: 3 | Blocks: 5 | References: `gift.py:108-115, 1598-1866, 1904-2510, 2319-3004`; `blivedm/handlers.py:50-175`; `blivedm/clients/ws_base.py:82-120`.
  Acceptance criteria (agent-executable): replayed representative event fixtures produce identical repository calls/values; the dispatch contract in `blivedm` remains unchanged; scheduler smoke tests start/cancel cleanly without external network.
  QA scenarios: happy: replay gift/guard/SC/danmaku events and verify counters/fields; failure: malformed/unknown event and failed HTTP response preserve skip/log behavior without corrupting queues. Evidence `.omo/evidence/task-4-vr-douchong-modularization-assessment.json`.
  Commit: Y | refactor(ingestion): separate protocol adaptation from monitoring
- [x] 5. Extract API/reporting and preserve launcher/CLI compatibility
  What to do / Must NOT do: Move FastAPI route definitions, input parsing, auth and response shaping into API/reporting modules; move all four archive functions into a maintenance service; keep `archive_attention` scheduler-only and keep the CLI invoking only its current three functions. Assign `MAIN_LOOP`, `_run_in_main_loop`, `_run_api_server` and startup ordering explicitly to bootstrap/API. Keep `gift.py` as executable compatibility facade and retain all route paths, response keys, status codes and current/archive branching.
  Parallelization: Wave 4 | Blocked by: 4 | Blocks: 6 | References: `gift.py:226-584, 3005-3712`; `gift.py:3151-3712`; `migrate_sc_archive.py:1-42`; `api.md`; `readme.md:49-63`.
  Acceptance criteria (agent-executable): FastAPI client before/after JSON and status snapshots are equal for every route, including `/gift`'s `current_concurrency` and `/gift/by_month`'s lack of it; `python migrate_sc_archive.py --month 202401` still invokes exactly three archive operations; direct `python gift.py` still initializes the loop, API thread and workers in the same order.
  QA scenarios: happy: current and historical route fixtures return identical payload shapes; failure: invalid query/body/auth and missing archive table retain existing errors/fallbacks. Evidence `.omo/evidence/task-5-vr-douchong-modularization-assessment.json`.
  Commit: Y | refactor(api): split routes and archive while retaining compatibility entrypoints
- [x] 6. Repackage application modules under app/ and make main.py the launcher
  What to do / Must NOT do: Move all production Python modules/packages currently at repository root (`gift.py`, `config.py`, `database.py`, `bootstrap.py`, `api_app.py`, `archive_service.py`, `bilibili_gateway.py`, `event_ingestion.py`, `monitoring_jobs.py`, `room_config.py`, `room_lifecycle.py`, `runtime_state.py`, `models/`, `repositories/`, `blivedm/`, and `migrate_sc_archive.py`) under `app/`; add root `main.py` that starts the same application. Update imports, tests, docs and CLI module paths. Keep root `.env`, existing `rooms.json`, documentation, dependency/config files and data files; do not rename the room config or change any DB/API behavior.
  Parallelization: Wave 4 | Blocked by: 5 | Blocks: 7 | References: current root tree; `gift.py` launcher/facade; `readme.md:49-63`; `.env.example:25-29`; `migrate_sc_archive.py:1-42`.
  Acceptance criteria (agent-executable): from repository root, `python main.py` is the only production startup command; `python -c "import app"` succeeds without DDL/network; no production `.py` module/package listed above remains at root; `pytest` imports resolve through `app`; `rooms.json` path and all API/schema contracts remain unchanged.
  QA scenarios (name the exact tool + invocation): happy: controlled fixture import and `python main.py` startup wiring resolve without DB/network; failure: `python -c "import app.gift"` does not start a server or mutate schema, and `python app/migrate_sc_archive.py --help` resolves without root-module imports. Evidence `.omo/evidence/task-6-vr-douchong-modularization-assessment.json`.
  Commit: Y | refactor(layout): move application modules under app and add main launcher
- [ ] 7. Run complete regression and maintenance handoff
  What to do / Must NOT do: Verify the full surface, document module ownership and extension rules, and remove only obsolete internal imports after compatibility checks. Do not touch unrelated dirty-worktree files or delete `psp_rooms.json`; verify its checksum remains unchanged.
  Parallelization: Wave 4 | Blocked by: 6 | Blocks: final wave | References: all paths above; `.gitignore:1-217`; `readme.md:14-63`.
  Acceptance criteria (agent-executable): compile/import checks pass; contract tests pass; schema diff is empty; all routes and CLI import; module dependency graph has no API/transport import in persistence/domain.
  QA scenarios: happy: start the service against staging configuration and call each endpoint; failure: stop/restart with an interrupted session and verify recovery/archiving behavior. Evidence `.omo/evidence/task-6-vr-douchong-modularization-assessment.json`.
  Commit: Y | chore(refactor): complete modularization verification and handoff
  Current status: BLOCKED by external runtime prerequisites, not by offline refactor checks. Full service startup failed on MySQL authentication; direct read-only Bilibili gateway probe returned HTTP 412 with non-JSON response. Resume this todo after valid DB credentials/network access are available.

## Final verification wave
> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.
- [ ] F1. Plan compliance audit
- [ ] F2. Code quality review
- [ ] F3. Real manual QA
- [ ] F4. Scope fidelity

## Commit strategy

Use one small commit per extraction wave, never mix line-ending normalization or unrelated dirty-worktree changes. Each commit must pass its wave's characterization/import checks; the final commit contains only compatibility cleanup and documentation.

## Success criteria

- `gift.py` is reduced to a compatibility launcher/orchestrator rather than the owner of DB, ingestion, jobs and API implementation.
- All current routes, payload keys, room JSON keys, schema/table/column/index names, archive suffixes and transaction boundaries remain unchanged.
- `migrate_sc_archive.py` continues to run through the extracted archive service.
- Characterization, staging-DB, import/compile and endpoint regression checks pass, with no live external API dependency in automated tests.
- New functionality can be added behind service/repository/integration boundaries without adding more unrelated responsibilities to the launcher.
