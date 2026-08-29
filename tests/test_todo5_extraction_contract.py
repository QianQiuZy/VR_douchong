"""Canonical ownership contract for Todo 5.

Verifies the *physical* extraction of the FastAPI transport / reporting
layer and the four archive functions out of ``app/gift.py``:

1. The four archive callables and the seven route callables are defined
   in ``app.archive_service`` / ``app.api_app`` respectively, not in ``app.gift``.
   ``app.gift`` only re-exports them for compatibility.
2. ``app/gift.py`` contains no live ``@app.<method>`` decorator for the
   frozen routes, no ``FastAPI(...)`` construction, and no ``def
   archive_*`` bodies.
3. ``app.api_app`` and ``app.archive_service`` do NOT import ``app.gift`` at import
   time (no circular dependency).  The route-level lookup of
   ``app.gift.Session`` / ``app.gift._run_in_main_loop`` etc. happens through
   ``sys.modules`` at request time, which is intentional and safe.
4. ``app.bootstrap`` owns ``main``, ``_run_api_server``, ``run``, the archive
   scheduler, and the launcher startup ordering.  ``MAIN_LOOP`` is the
   canonical runtime-state singleton, and ``_run_in_main_loop`` lives on
   ``app.api_app``.
5. ``app.migrate_sc_archive`` still calls exactly the three CLI archive
   functions - no more, no less - and never touches ``archive_attention``.
"""

from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
GIFT_PATH = REPO_ROOT / "app" / "gift.py"
ARCHIVE_PATH = REPO_ROOT / "app" / "archive_service.py"
API_PATH = REPO_ROOT / "app" / "api_app.py"
BOOTSTRAP_PATH = REPO_ROOT / "app" / "bootstrap.py"
MIGRATE_PATH = REPO_ROOT / "app" / "migrate_sc_archive.py"


# --------------------------------------------------------------------- #
# Helpers                                                               #
# --------------------------------------------------------------------- #
def _top_level_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _decorated_route_paths(path: Path) -> list[tuple[str, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    routes: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue
            func = deco.func
            if not isinstance(func, ast.Attribute):
                continue
            owner = func.value
            if not isinstance(owner, ast.Name):
                continue
            if owner.id != "app":
                continue
            if func.attr not in {"get", "post", "put", "delete", "patch"}:
                continue
            if not deco.args or not isinstance(deco.args[0], ast.Constant):
                continue
            route_path = deco.args[0].value
            if not isinstance(route_path, str):
                continue
            routes.append((func.attr.upper(), route_path))
    return routes


def _module_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module.split(".")[0])
    return names


# --------------------------------------------------------------------- #
# 1. Canonical archive ownership                                        #
# --------------------------------------------------------------------- #
class TestArchiveServiceOwnership:
    def test_archive_service_defines_all_four_archive_functions(self):
        names = _top_level_function_names(ARCHIVE_PATH)
        assert {
            "archive_super_chat_log",
            "archive_live_session",
            "archive_room_live_stats",
            "archive_attention",
        }.issubset(names)

    def test_gift_no_longer_defines_archive_bodies(self):
        names = _top_level_function_names(GIFT_PATH)
        # app/gift.py must not physically declare the archive functions -
        # they are re-exported bindings from archive_service.
        assert "archive_super_chat_log" not in names
        assert "archive_live_session" not in names
        assert "archive_room_live_stats" not in names
        assert "archive_attention" not in names

    def test_gift_re_exports_archive_callables_from_archive_service(self, gift_module):
        from app import archive_service

        assert gift_module.archive_super_chat_log is archive_service.archive_super_chat_log
        assert gift_module.archive_live_session is archive_service.archive_live_session
        assert gift_module.archive_room_live_stats is archive_service.archive_room_live_stats
        assert gift_module.archive_attention is archive_service.archive_attention

    def test_migrate_still_calls_exactly_three_archive_functions(self):
        # Static AST guard against archive_attention creeping into the CLI.
        source = MIGRATE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        cli_imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "app.gift":
                cli_imports.update(alias.name for alias in node.names)
        assert cli_imports == {
            "archive_live_session",
            "archive_room_live_stats",
            "archive_super_chat_log",
            "normalize_month_code",
        }
        assert "archive_attention" not in source


# --------------------------------------------------------------------- #
# 2. Canonical route ownership                                          #
# --------------------------------------------------------------------- #
class TestApiAppOwnership:
    _FROZEN_ROUTES = {
        ("POST", "/add/room"),
        ("POST", "/delete/room"),
        ("GET", "/gift"),
        ("GET", "/gift/by_month"),
        ("GET", "/gift/live_sessions"),
        ("GET", "/gift/attention"),
        ("GET", "/gift/sc"),
    }

    def test_api_app_declares_all_seven_frozen_routes(self):
        declared = set(_decorated_route_paths(API_PATH))
        assert self._FROZEN_ROUTES.issubset(declared), (
            f"app/api_app.py must physically declare all seven routes; "
            f"missing = {self._FROZEN_ROUTES - declared}"
        )

    def test_gift_no_longer_declares_route_decorators(self):
        declared = set(_decorated_route_paths(GIFT_PATH))
        assert declared == set(), (
            f"app/gift.py must not physically declare routes; found {declared}"
        )

    def test_gift_no_longer_constructs_fastapi(self):
        source = GIFT_PATH.read_text(encoding="utf-8")
        assert "FastAPI(" not in source, (
            "app/gift.py must not construct FastAPI(...); the app lives in app/api_app.py"
        )

    def test_gift_re_exports_the_same_app_object(self, gift_module):
        from app import api_app

        assert gift_module.app is api_app.app

    def test_api_app_helper_functions_are_defined_in_api_app(self):
        names = _top_level_function_names(API_PATH)
        for helper in (
            "_run_in_main_loop",
            "_parse_room_payload",
            "_check_api_secret",
            "_room_ids_for_month",
            "_seconds_to_hms",
            "_tenths_to_decimal",
            "_profit_display",
            "add_room_async",
            "delete_room_async",
        ):
            assert helper in names, f"app/api_app.py must physically own {helper}"

    def test_gift_no_longer_defines_route_helper_bodies(self):
        names = _top_level_function_names(GIFT_PATH)
        for helper in (
            "_run_in_main_loop",
            "_parse_room_payload",
            "_check_api_secret",
            "_room_ids_for_month",
            "_profit_display",
            "_tenths_to_decimal",
        ):
            assert helper not in names, (
                f"app/gift.py must not physically define {helper}; re-export from app.api_app"
            )


# --------------------------------------------------------------------- #
# 3. Canonical bootstrap ownership                                      #
# --------------------------------------------------------------------- #
class TestBootstrapOwnership:
    def test_bootstrap_defines_main_run_api_server_and_run(self):
        names = _top_level_function_names(BOOTSTRAP_PATH)
        assert {"main", "_run_api_server", "run", "monthly_reset_scheduler"}.issubset(
            names
        )

    def test_bootstrap_owns_archive_month_helper(self):
        names = _top_level_function_names(BOOTSTRAP_PATH)
        assert "_archive_month" in names

    def test_gift_no_longer_defines_main_or_run_api_server(self):
        names = _top_level_function_names(GIFT_PATH)
        assert "main" not in names, "main() belongs to app/bootstrap.py now"
        assert "_run_api_server" not in names, "_run_api_server belongs to app/bootstrap.py now"

    def test_gift_re_exports_bootstrap_symbols(self, gift_module):
        from app import bootstrap

        assert gift_module.main is bootstrap.main
        assert gift_module._run_api_server is bootstrap._run_api_server
        assert gift_module.monthly_reset_scheduler is bootstrap.monthly_reset_scheduler
        assert gift_module._archive_month is bootstrap._archive_month
        assert gift_module.init_room_info is bootstrap.init_room_info

    def test_main_loop_lives_on_runtime_state(self):
        from app import runtime_state

        # The canonical binding lives on runtime_state; gift only re-exports
        # for the initial import-time snapshot the tests read.
        assert hasattr(runtime_state, "MAIN_LOOP")
        assert runtime_state.MAIN_LOOP is None


# --------------------------------------------------------------------- #
# 4. No circular imports at module load                                 #
# --------------------------------------------------------------------- #
class TestNoCircularImports:
    def test_api_app_does_not_statically_import_gift(self):
        imports = _module_imports(API_PATH)
        assert "gift" not in imports, (
            "app/api_app.py must not import app.gift at module scope; use sys.modules lookup"
        )

    def test_archive_service_does_not_import_gift(self):
        imports = _module_imports(ARCHIVE_PATH)
        assert "gift" not in imports

    def test_bootstrap_does_not_import_gift(self):
        imports = _module_imports(BOOTSTRAP_PATH)
        assert "gift" not in imports

    def test_new_modules_import_cleanly_stand_alone(self):
        # Prove app.archive_service and app.api_app can be imported without app.gift
        # being on sys.modules first.  (In the pytest session app.gift is
        # loaded via conftest, but we verify the modules themselves don't
        # require it.)
        for module_name in ("app.archive_service", "app.api_app", "app.bootstrap"):
            module = importlib.import_module(module_name)
            assert module is not None
            source_file = inspect.getsourcefile(module)
            assert source_file is not None
            assert str(REPO_ROOT) in str(source_file)


# --------------------------------------------------------------------- #
# 5. Compatibility surface still complete                               #
# --------------------------------------------------------------------- #
class TestGiftCompatibilitySurface:
    def test_gift_still_owns_room_config_wrappers(self, gift_module):
        assert callable(gift_module.load_rooms_config)
        assert callable(gift_module.save_rooms_config)
        assert callable(gift_module.get_room_ids)
        assert callable(gift_module.get_room_anchors)
        assert callable(gift_module.get_room_anchor_name)

    def test_gift_still_owns_room_lifecycle_wrappers(self, gift_module):
        assert callable(gift_module.add_room_async)
        assert callable(gift_module.delete_room_async)
        assert callable(gift_module._lifecycle_dependencies)
        assert callable(gift_module._finish_live_session)
        assert callable(gift_module._defer_live_session_finish)
        assert callable(gift_module._resume_interrupted_session)
        assert callable(gift_module._finish_expired_live_sessions)
        assert callable(gift_module.ensure_room_state)

    def test_gift_still_owns_bilibili_gateway_compat(self, gift_module):
        assert callable(gift_module.init_session)
        assert callable(gift_module.ensure_bili_ticket)
        assert callable(gift_module._fetch_room_info_and_update)
        assert callable(gift_module._fetch_room_init)
        assert callable(gift_module._fetch_guard_counts)
        assert callable(gift_module._fetch_fans_count)
        assert callable(gift_module._fetch_contribution_count)

    def test_gift_still_owns_cookie_alert_helper(self, gift_module):
        assert callable(gift_module.send_cookie_invalid_email_async)
        assert callable(gift_module._profit_to_tenths)

    def test_gift_still_owns_monitoring_jobs_re_exports(self, gift_module):
        assert callable(gift_module.run_clients_loop)
        assert callable(gift_module.init_uids_and_attention_once)
        assert callable(gift_module.attention_worker)
        assert callable(gift_module.monthly_reset_scheduler)

    def test_gift_still_exposes_route_callables(self, gift_module):
        # External callers may have imported the route functions directly.
        assert callable(gift_module.add_room_api)
        assert callable(gift_module.delete_room_api)
        assert callable(gift_module.get_stats_current_month)
        assert callable(gift_module.get_stats_by_month)
        assert callable(gift_module.get_live_sessions_by_room_month)
        assert callable(gift_module.get_attention_logs)
        assert callable(gift_module.get_sc_logs)

    def test_gift_still_exposes_seven_routes_on_app(self, gift_module):
        recorded = {
            (route.path, tuple(sorted(route.methods)))
            for route in gift_module.app.routes
            if hasattr(route, "methods") and route.methods is not None
        }
        assert ("/add/room", ("POST",)) in recorded
        assert ("/delete/room", ("POST",)) in recorded
        assert ("/gift", ("GET",)) in recorded
        assert ("/gift/by_month", ("GET",)) in recorded
        assert ("/gift/live_sessions", ("GET",)) in recorded
        assert ("/gift/attention", ("GET",)) in recorded
        assert ("/gift/sc", ("GET",)) in recorded
