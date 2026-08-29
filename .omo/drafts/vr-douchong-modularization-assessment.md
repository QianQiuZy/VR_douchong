---
slug: vr-douchong-modularization-assessment
status: awaiting-approval
intent: clear
review_required: false
pending-action: execute the future behavior-preserving extraction plan only after explicit approval
approach: split gift.py by dependency direction, keep a compatibility launcher, preserve all DB/JSON/API contracts, and verify each extraction with characterization and endpoint checks
---

# Draft: vr-douchong-modularization-assessment

## Components (topology ledger)
<!-- Lock the SHAPE before depth. One row per top-level component that can succeed or fail independently. -->
<!-- id | outcome (one line) | status: active|deferred | evidence path -->
- bootstrap-config | Loads environment, room configuration, DB/session and starts the application | active | gift.py:48-105, 1335-1494, 3073-3712
- persistence | Owns ORM models, repositories and archive-table operations without changing schema | active | gift.py:226-1333
- room-runtime | Owns room lifecycle, session state and in-memory runtime state | active | gift.py:1523-1629, 1867-2123
- ingestion | Converts blivedm events into domain operations; transport remains in blivedm | active | gift.py:1632-1866; blivedm/handlers.py:50-175
- monitoring-jobs | Owns polling, queues, workers, reconnect, ticket refresh and monthly scheduling | active | gift.py:2319-3071
- api-reporting | Owns FastAPI transport, validation and response shaping | active | gift.py:3106-3701

## Open assumptions (announced defaults)
<!-- Record any default you adopt instead of asking, so the user can veto it at the gate. -->
<!-- assumption | adopted default | rationale | reversible? -->
- Keep `blivedm/` as a dependency boundary | Do not refactor bundled protocol/model files in the first pass | It already separates websocket transport, dispatch and payload models; changing it multiplies protocol risk | yes
- Preserve compatibility imports from `gift.py` during migration | Make `gift.py` a thin launcher/compatibility facade before removing names | `migrate_sc_archive.py` and operational scripts import from `gift` directly | yes
- Characterization tests before extraction | Capture current API JSON, error statuses, room-config behavior and archive behavior first | No test suite currently exists; behavior must be observed before movement | yes
- Preserve transaction granularity | Keep per-operation ORM commits and per-month archive transactions | Current data-loss/locking behavior depends on these boundaries | no, unless separately approved
- Make DB/schema bootstrap callable | Keep direct `python gift.py` startup equivalent, but stop plain module imports from issuing DDL | Importing `gift` currently calls `create_all` and `ALTER TABLE`, making tests and the archive CLI unsafe | yes
- Keep archive CLI scope unchanged | Extract all four archive functions, but keep `migrate_sc_archive.py` invoking only its current three | `archive_attention` is used by the in-process monthly scheduler, not the CLI | yes
- Single owner for queues and runtime state | Put queues and mutable room/session caches behind one runtime-state module before extracting consumers | Room lifecycle and ingestion share these objects; parallel extraction otherwise risks circular imports | yes

## Findings (cited - path:lines)

- `gift.py` is 3,712 lines and combines environment loading, DB engine/session, ORM models, archive SQL, room JSON state, Bilibili HTTP calls, websocket event handlers, queues/workers, schedulers, FastAPI routes and the launcher (`gift.py:1-3712`).
- Import-time side effects currently include environment loading, room loading, `Base.metadata.create_all(engine)`, and `ensure_runtime_schema()` (`gift.py:48-65, 1340-1412, 1470-1494`).
- Runtime state is shared through mutable globals such as `ROOM_IDS`, `ROOM_UIDS`, `CURRENT_SESSIONS`, `ROOM_CLIENTS`, `LAST_STATUS`, `LIVE_INFO`, queues and metric caches (`gift.py:1335-1551, 1873-1896`).
- Persistence has eight core tables/models: `room_info`, `attention`, `room_stats_monthly`, `room_blind_box_monthly`, `room_live_stats`, `live_session`, and `super_chat_log`, plus month-suffixed archive tables (`gift.py:619-1333, 178-584`).
- Database and external payload names are public contracts: DB columns include `room_id`, `month`, `date`, `guard_1/2/3`, `fans_count`, `start_*`, `end_*`, `blind_box_*`, `danmaku_count`, `send_time`, `uname`, `uid`, `price`, `message`; room config uses `room_ids` and `room_anchors` (`gift.py:226-584, 619-1333, 1340-1398; rooms.json:1-122`).
- API surface consists of `/add/room`, `/delete/room`, `/gift`, `/gift/by_month`, `/gift/live_sessions`, `/gift/attention`, and `/gift/sc`, including current-vs-archive branching and response keys (`gift.py:3151-3701`).
- Live ingestion is coupled to persistence through `MyHandler`; Bilibili command dispatch is provided by `blivedm.BaseHandler` (`gift.py:1632-1866; blivedm/handlers.py:50-175`).
- Long-running orchestration is centralized in `main()` and starts clients, status monitoring, workers, schedulers, archive, reconnect, ticket and concurrency jobs together (`gift.py:3005-3104`).
- There is no test suite, package metadata, Dockerfile or dedicated migration framework; runtime is documented as `pip install -r requirements.txt` then `python gift.py` (`requirements.txt:1-7; readme.md:14-23, 49-63`).
- `migrate_sc_archive.py` imports archive functions from `gift`, so extracting archive code without a compatibility facade will break the maintenance CLI (`migrate_sc_archive.py:11-42`).
- `migrate_sc_archive.py` invokes three archive functions only; `archive_attention` is used by the in-process monthly scheduler (`migrate_sc_archive.py:11-32; gift.py:3005-3043`).
- The API thread/event-loop bridge is `MAIN_LOOP`, `_run_in_main_loop`, `_run_api_server` and `main()` (`gift.py:3074-3114, 3703-3712`).
- `COMMON_NOTICE_GIFT_COIN_MAP` is a business rule used by `MyHandler._on_common_notice_danmaku` (`gift.py:108-115, 1746-1770`).
- `ATTENTION_DAILY_ROOM_SLEEP_SECONDS` is read from the environment but omitted from `.env.example` (`gift.py:104; .env.example:1-29`).
- `/gift` includes `current_concurrency`, while `/gift/by_month` does not (`gift.py:3232-3252, 3309-3328`).
- `api.md` documents the broader API contract and must be reviewed alongside the shorter README (`api.md; readme.md:49-63`).
- The bundled websocket stack contains a modern aiohttp compatibility risk (`blivedm/clients/ws_base.py:101`); it is recorded as a residual risk and is out of scope for this behavior-preserving decomposition.
- Dependencies are unpinned in `requirements.txt:1-7`; reproducible validation should capture the actual environment, but changing dependency policy is out of scope.
- The worktree already contained line-ending-only modifications in twelve tracked files; `git diff --ignore-space-at-eol --stat` was empty. These changes are out of scope and must not be overwritten.

## Decisions (with rationale)

- Use six top-level application areas: bootstrap/config, persistence, room-runtime, ingestion, monitoring-jobs, and API/reporting. This is enough separation for independent maintenance without prematurely creating a package per function.
- Split persistence by aggregate/repository, not by individual table helper: room info/config, monthly stats, live session, attention, SC logs, live duration, and archive service. This keeps related writes together while retaining the existing fields and commit behavior.
- Keep domain/application services between API/handlers and repositories. API routes should validate/dispatch and shape the existing payloads; event handlers should normalize messages and call services, not issue SQL directly.
- Inject or group runtime state behind a room-scoped state object during later extraction, but do not change state semantics, queue ordering, retry counts, polling intervals, or grace-period logic in the first pass.
- Keep `gift.py` as the executable compatibility entrypoint initially. The final desired shape is a thin launcher that re-exports only intentionally supported names while new modules own implementation.
- Assign runtime state/queues a single owner and assign `MAIN_LOOP`/`_run_in_main_loop`/`_run_api_server` to the bootstrap/API boundary.
- Treat `blivedm/` as unchanged protocol code; the application `MyHandler` moves out of `gift.py`, not `blivedm/handlers.py`.
- Keep `archive_attention` scheduler-only in this scope; do not add it to the CLI.

## Scope IN

- Behavior-preserving decomposition of `gift.py` into maintainable modules.
- Explicit boundaries for DB bootstrap/models/repositories/archive, room lifecycle, Bilibili gateway, event ingestion, monitoring jobs, FastAPI routes, and launcher/CLI.
- Characterization coverage and contract checks for existing routes, error responses, room JSON, schema/table names, archive suffixes and transaction boundaries.
- An extraction order that allows each step to be imported and rolled back independently.

## Scope OUT (Must NOT have)

- No DB table, column, index, primary-key, archive suffix, JSON key, route path, response field, status code or existing message-contract changes.
- No new frontend; this repository has no HTML/JS/TS/Vue/React UI. Its current user surface is the JSON API and live-ingestion runtime.
- No redesign of `blivedm` protocol parsing/models in the first pass.
- No change to business formulas, gift mappings, guard numbering, time windows, polling intervals, retry behavior, queue semantics, archive deletion behavior or import/launcher compatibility unless separately approved.
- The only allowed import-lifecycle adjustment is making schema bootstrap explicit so a plain import does not mutate the database; direct launch must still initialize before serving/collecting.
- No deletion/merging of `psp_rooms.json`; it was not found referenced and should be treated as an unresolved data artifact, not silently removed.
- No opportunistic dependency upgrade, packaging migration, async DB rewrite, schema migration framework adoption or frontend implementation.

## Open questions

- None block the assessment. The owner decision that remains before implementation is whether to retain the `gift.py` compatibility facade permanently or remove it after a separately approved deprecation period; default is to retain it through the first complete migration.

## Approval gate
status: awaiting-approval
pending: explicit approval before executing any source-code extraction
<!-- This turn only delivers the assessment; no product source was edited. -->
<!-- That durable record is the loop guard: on a later turn read it and resume at the gate instead of re-running exploration. -->
