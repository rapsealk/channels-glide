"""Integration tests against a real Valkey/Redis server via GLIDE.

Skipped unless ``VALKEY_URL`` is set (e.g. ``redis://localhost:6379/0``).
CI runs them with a Valkey service container.
"""

from __future__ import annotations

import os

import pytest

from channels_glide import GlideChannelLayer

pytestmark = pytest.mark.skipif(
    not os.environ.get("VALKEY_URL"), reason="VALKEY_URL not set"
)


def _layer() -> GlideChannelLayer:
    url = os.environ["VALKEY_URL"]
    host = url.split("//")[-1].split("/")[0]
    h, _, p = host.partition(":")
    return GlideChannelLayer(prefix="itest", hosts=[(h, int(p or 6379))])


async def test_roundtrip() -> None:
    layer = _layer()
    await layer.send("itest.channel", {"hello": "world"})
    assert await layer.receive("itest.channel") == {"hello": "world"}
    await layer.flush()
    await layer.close()


async def test_groups() -> None:
    layer = _layer()
    await layer.group_add("itest-room", "itest.member")
    await layer.group_send("itest-room", {"event": "ping"})
    assert await layer.receive("itest.member") == {"event": "ping"}
    await layer.group_discard("itest-room", "itest.member")
    await layer.flush()
    await layer.close()
