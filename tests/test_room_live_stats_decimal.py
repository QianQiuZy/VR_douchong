from contextlib import contextmanager

import pytest
from sqlalchemy import Integer, Numeric
from sqlalchemy.dialects.mysql import DECIMAL, FLOAT
from sqlalchemy.exc import SQLAlchemyError

from app import database
from app.models import RoomLiveStats


class NoRows:
    def first(self):
        return None


def test_room_live_stats_model_uses_exact_money_types():
    # Given a fresh database created from ORM metadata
    columns = RoomLiveStats.__table__.columns

    # When inspecting its three money columns
    # Then their precision and scale match the production schema
    assert [(columns[name].type.precision, columns[name].type.scale) for name in (
        "gift", "guard", "super_chat"
    )] == [(20, 1), (20, 0), (20, 0)]
    assert all(isinstance(columns[name].type, Numeric) and not isinstance(columns[name].type, FLOAT)
               for name in ("gift", "guard", "super_chat"))


@pytest.mark.parametrize("already_decimal", [False, True])
def test_restart_leaves_existing_hot_and_archive_types_unchanged(monkeypatch, already_decimal):
    # Given hot and historical tables with either FLOAT or target DECIMAL types
    statements = []
    names = ["room_live_stats", "room_live_stats_202508"]

    class Inspector:
        def get_table_names(self):
            return names

        def get_columns(self, _name):
            types = (DECIMAL(20, 1), DECIMAL(20, 0), DECIMAL(20, 0)) if already_decimal else (FLOAT(), FLOAT(), FLOAT())
            return [{"name": name, "type": kind} for name, kind in zip(("gift", "guard", "super_chat"), types)] + [
                {"name": name, "type": Integer()} for name in ("payer_count", "steel_coin_count")
            ]

    class Connection:
        def execute(self, statement):
            statements.append(str(statement))
            return NoRows()

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(database, "engine", Engine())
    monkeypatch.setattr(database, "inspect", lambda _engine: Inspector())

    # When the launcher checks the existing database schema
    database.ensure_runtime_schema()

    # Then existing monetary columns are never touched at startup
    assert statements == []


def test_restart_does_not_attempt_to_convert_float_columns(monkeypatch):
    # Given a table with old FLOAT columns and a database rejecting all writes
    class Inspector:
        def get_table_names(self):
            return ["room_live_stats"]

        def get_columns(self, _name):
            return [{"name": name, "type": FLOAT()} for name in ("gift", "guard", "super_chat")] + [
                {"name": name, "type": Integer()} for name in ("payer_count", "steel_coin_count")
            ]

    class Connection:
        def execute(self, statement):
            raise SQLAlchemyError("unexpected schema write")

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(database, "engine", Engine())
    monkeypatch.setattr(database, "inspect", lambda _engine: Inspector())

    # When startup inspects existing columns
    database.ensure_runtime_schema()
    # Then it does not issue a schema write


def test_restart_does_not_read_or_truncate_existing_gifts(monkeypatch):
    # Given a FLOAT table whose existing gifts require manual truncation
    statements = []

    class Inspector:
        def get_table_names(self):
            return ["room_live_stats"]

        def get_columns(self, _name):
            return [{"name": name, "type": FLOAT()} for name in ("gift", "guard", "super_chat")] + [
                {"name": name, "type": Integer()} for name in ("payer_count", "steel_coin_count")
            ]

    class Connection:
        def execute(self, statement):
            statements.append(str(statement))
            return NoRows()

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(database, "engine", Engine())
    monkeypatch.setattr(database, "inspect", lambda _engine: Inspector())

    # When startup inspects the schema
    database.ensure_runtime_schema()
    # Then it leaves existing values for the manual SQL migration
    assert statements == []
