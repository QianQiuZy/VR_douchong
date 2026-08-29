"""Characterization: canonical runtime state remains compatible with gift."""

from __future__ import annotations

import asyncio

from app import runtime_state


class TestRuntimeStateCompatibility:
    def test_queue_singletons_are_shared_with_gift(self, gift_module):
        # Given: the import-safe gift compatibility surface.
        expected_queues = (
            "GUARD_FANS_QUEUE",
            "ATTENTION_QUEUE",
            "DAILY_ATTENTION_QUEUE",
            "DAILY_GUARD_QUEUE",
            "DAILY_FANS_QUEUE",
        )

        # When: each queue is resolved from the canonical state module.
        actual_queues = tuple(getattr(runtime_state, name) for name in expected_queues)

        # Then: gift re-exports the same empty asyncio.Queue instances.
        assert all(isinstance(queue, asyncio.Queue) and queue.empty() for queue in actual_queues)
        assert actual_queues == tuple(getattr(gift_module, name) for name in expected_queues)

    def test_room_registry_singletons_are_shared_with_gift(self, gift_module):
        # Given: canonical room registry containers.
        registry_names = ("ROOM_IDS", "ROOM_ANCHORS", "ROOM_UIDS", "CURRENT_SESSIONS", "ROOM_CLIENTS")

        # When: the legacy gift attributes are inspected.
        gift_registries = tuple(getattr(gift_module, name) for name in registry_names)

        # Then: legacy imports still point at the canonical mutable containers.
        assert gift_registries == tuple(getattr(runtime_state, name) for name in registry_names)
