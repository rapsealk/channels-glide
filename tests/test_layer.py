"""Unit tests for GlideChannelLayer against the in-memory fake client."""

from __future__ import annotations

import asyncio
import time

import pytest
from channels.exceptions import ChannelFull

from channels_glide import GlideChannelLayer


async def test_send_receive_roundtrip(layer: GlideChannelLayer) -> None:
    await layer.send("test.channel", {"hello": "world"})
    message = await layer.receive("test.channel")
    assert message == {"hello": "world"}


async def test_send_receive_preserves_types(layer: GlideChannelLayer) -> None:
    payload = {"ints": [1, 2, 3], "text": "héllo", "nested": {"a": True}}
    await layer.send("test.channel", payload)
    assert await layer.receive("test.channel") == payload


async def test_local_channel_roundtrip(layer: GlideChannelLayer) -> None:
    channel = await layer.new_channel()
    await layer.send(channel, {"kind": "local"})
    message = await layer.receive(channel)
    assert message == {"kind": "local"}
    # The wrapper key must not leak into the received message.
    assert "__asgi_channel__" not in message


async def test_new_channel_format(layer: GlideChannelLayer) -> None:
    channel = await layer.new_channel()
    assert channel.startswith("specific.")
    # The "!" separates the non-local part from the process-local suffix.
    assert layer.non_local_name(channel).endswith(layer.client_prefix + "!")
    assert layer.client_prefix in channel
    # Specific channels are only receivable by this layer instance.
    with pytest.raises(AssertionError):
        other = GlideChannelLayer(prefix="test")
        await other.receive(channel)


async def test_capacity_raises_channel_full(fake_client) -> None:  # type: ignore[no-untyped-def]
    from channels_glide.glide import GlideChannelLayer as Layer

    tight = Layer(prefix="test", capacity=1, client=fake_client)  # type: ignore[arg-type]
    await tight.send("test.channel", {"n": 1})
    with pytest.raises(ChannelFull):
        await tight.send("test.channel", {"n": 2})


async def test_expiry_drops_old_messages(
    layer: GlideChannelLayer, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = time.time()
    await layer.send("test.channel", {"old": True})
    # Jump past expiry; the next send must discard the aged-out message.
    monkeypatch.setattr(time, "time", lambda: now + 3600)
    await layer.send("test.channel", {"new": True})
    message = await layer.receive("test.channel")
    assert message == {"new": True}


async def test_group_add_send_discard(
    layer: GlideChannelLayer, fake_client
) -> None:  # type: ignore[no-untyped-def]
    await layer.group_add("room", "test.member")
    await layer.group_send("room", {"event": "ping"})
    message = await layer.receive("test.member")
    assert message == {"event": "ping"}
    await layer.group_discard("room", "test.member")
    await layer.group_send("room", {"event": "pong"})
    # Nothing is queued for the discarded channel anymore.
    zset = fake_client.zsets.get("test" + "test.member", {})
    assert len(zset) == 0

async def test_group_send_dedupes_local_channels(
    layer: GlideChannelLayer, fake_client
) -> None:  # type: ignore[no-untyped-def]
    first = await layer.new_channel()
    second = await layer.new_channel()
    # Same non-local part -> one queue key carrying both channels.
    await layer.group_add("room", first)
    await layer.group_add("room", second)
    await layer.group_send("room", {"event": "fanout"})
    queue_key = "test" + layer.non_local_name(first)
    zset = fake_client.zsets.get(queue_key, {})
    assert len(zset) == 1
    first_message = await layer.receive(first)
    second_message = await layer.receive(second)
    assert first_message == {"event": "fanout"}
    assert second_message == {"event": "fanout"}


async def test_flush_clears_everything(
    layer: GlideChannelLayer, fake_client
) -> None:  # type: ignore[no-untyped-def]
    await layer.send("test.channel", {"n": 1})
    await layer.group_add("room", "test.channel")
    await layer.flush()
    assert "test" + "test.channel" not in fake_client.zsets
    assert "test:group:room" not in fake_client.zsets
    await layer.send("test.channel", {"n": 2})
    assert await layer.receive("test.channel") == {"n": 2}


async def test_send_requires_dict(layer: GlideChannelLayer) -> None:
    with pytest.raises(AssertionError):
        await layer.send("test.channel", "not-a-dict")  # type: ignore[arg-type]


async def test_multiple_messages_in_order(layer: GlideChannelLayer) -> None:
    for i in range(3):
        await layer.send("test.channel", {"i": i})
    received = [await layer.receive("test.channel") for _ in range(3)]
    assert received == [{"i": 0}, {"i": 1}, {"i": 2}]


async def test_receive_serializes_waiters(layer: GlideChannelLayer) -> None:
    channel = await layer.new_channel()
    task = asyncio.ensure_future(layer.receive(channel))
    await asyncio.sleep(0)
    await layer.send(channel, {"late": True})
    message = await asyncio.wait_for(task, timeout=2)
    assert message == {"late": True}
