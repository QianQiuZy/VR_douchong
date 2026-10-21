"""Imports stay offline and independent of an already-loaded launcher."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "module_name", [
        "app.api_app", "app.archive_service", "app.bootstrap", "app.gift",
        "app.logging_setup", "app.api_request_logging",
    ]
)
def test_import_is_offline_in_a_fresh_process(module_name):
    environment = os.environ.copy()
    environment["ENV_FILE"] = str(REPO_ROOT / "tests" / "_fixtures" / "empty.env")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    program = """
import importlib
import logging
import socket
import sys
from unittest.mock import Mock, patch

from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

assert 'app.gift' not in sys.modules
forbidden = Mock(side_effect=AssertionError('import must not connect or create tables'))
with (
    patch.object(socket.socket, 'connect', forbidden),
    patch.object(Engine, 'connect', forbidden),
    patch.object(MetaData, 'create_all', forbidden),
    patch.object(logging.FileHandler, '_open', forbidden),
):
    importlib.import_module(sys.argv[1])
forbidden.assert_not_called()
if sys.argv[1] != 'app.gift':
    assert 'app.gift' not in sys.modules, 'module import must not load the launcher'
"""
    result = subprocess.run(
        [sys.executable, "-c", program, module_name],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_common_notice_gift_coin_values(gift_module):
    assert gift_module.COMMON_NOTICE_GIFT_COIN_MAP == {
        "干杯之旅": 10000,
        "启航之旅": 100000,
        "友谊的小船": 4900,
        "冲浪": 89900,
        "海湾之旅": 799900,
        "鸿运小电视": 1000000,
    }


def test_fastapi_app_disables_public_docs_endpoints(gift_module):
    app = gift_module.app
    assert app.docs_url is None
    assert app.redoc_url is None
    assert app.openapi_url is None
