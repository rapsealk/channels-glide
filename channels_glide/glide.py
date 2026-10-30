"""Django Channels channel layer backed by Valkey GLIDE.

A port of ``channels_redis.core.RedisChannelLayer`` semantics onto the Valkey
GLIDE client: expiring sorted-set message queues, backup queues for
crash-safe receives, and group support — driven by a single GLIDE client
instead of a sharded connection ring.
"""

from __future__ import annotations

import asyncio
import collections
import datetime
import functools
import logging
import time
import urllib.parse
import uuid
from collections.abc import Mapping
from typing import Any

import msgpack
from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer
from glide import (
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    RangeByIndex,
    ScoreBoundary,
    Script,
    ServerCredentials,
)

__all__ = ["GlideChannelLayer"]

logger = logging.getLogger(__name__)

# Restore backed-up (in-flight) messages into the main queue before the
# blocking pop, so a cancelled receiver never loses a message.
# ARGV[1] = main queue key, ARGV[2] = backup queue key.
_CLEANUP_SCRIPT = Script(  # type: ignore[no-untyped-call]
    """
    local backed_up = redis.call('ZRANGE', ARGV[2], 0, -1, 'WITHSCORES')
    for i = #backed_up, 1, -2 do
        redis.call('ZADD', ARGV[1], backed_up[i], backed_up[i - 1])
    end
    redis.call('DEL', ARGV[2])
"""
)

# Scatter a message onto every channel queue in the group that is under
# capacity. KEYS = channel queue keys; ARGV = [message per key...,
# capacity per key..., current_time, expiry]. Returns the number of
# channels over capacity.
_GROUP_SEND_SCRIPT = Script(  # type: ignore[no-untyped-call]
    """
    local over_capacity = 0
    local current_time = ARGV[#ARGV - 1]
    local expiry = ARGV[#ARGV]
    for i = 1, #KEYS do
        if redis.call('ZCOUNT', KEYS[i], '-inf', '+inf') < tonumber(ARGV[i + #KEYS]) then
            redis.call('ZADD', KEYS[i], current_time, ARGV[i])
            redis.call('EXPIRE', KEYS[i], expiry)
        else
            over_capacity = over_capacity + 1
        end
    end
    return over_capacity
"""
)

# Delete every key under a prefix (test/dev flush).
_FLUSH_SCRIPT = Script(  # type: ignore[no-untyped-call]
    """
    local keys = redis.call('keys', ARGV[1])
    for i = 1, #keys, 5000 do
        redis.call('del', unpack(keys, i, math.min(i + 4999, #keys)))
    end
"""
)

# Random prefix length prepended to each serialized payload so identical
# messages stay unique members of their sorted set.
_RANDOM_PREFIX_LENGTH = 12


class ChannelLock:
    """
    Helper class for per-channel locking.

    Once a lock is released and has no waiters, it is deleted to keep the
    map small.
    """

    def __init__(self) -> None:
        self.locks: collections.defaultdict[str, asyncio.Lock] = collections.defaultdict(
            asyncio.Lock
        )
        self.wait_counts: collections.defaultdict[str, int] = collections.defaultdict(int)

    async def acquire(self, channel: str) -> bool:
        """Acquire the lock for the given channel."""
        self.wait_counts[channel] += 1
        return await self.locks[channel].acquire()

    def locked(self, channel: str) -> bool:
        """Return ``True`` if the lock for the given channel is acquired."""
        return self.locks[channel].locked()

    def release(self, channel: str) -> None:
        """Release the lock for the given channel."""
        self.locks[channel].release()
        self.wait_counts[channel] -= 1
        if self.wait_counts[channel] < 1:
            del self.locks[channel]
            del self.wait_counts[channel]


class BoundedQueue(asyncio.Queue[dict[str, Any]]):
    def put_nowait(self, item: dict[str, Any]) -> None:
        if self.full():
            # See https://github.com/django/channels_redis/issues/212 — if we
            # get here, this specific consumer has likely stopped reading.
            # Drop messages beyond the channel capacity rather than grow
            # without bound.
            self.get_nowait()
        return super().put_nowait(item)


def _encode_datetime(obj: Any) -> float:
    if isinstance(obj, datetime.datetime):
        return obj.timestamp()
    raise TypeError(f"Cannot serialize {type(obj)!r}")


class GlideChannelLayer(BaseChannelLayer):
    """
    Django Channels channel layer backed by Valkey GLIDE.

    Configured from the ``CONFIG`` block of ``CHANNEL_LAYERS``:

        CHANNEL_LAYERS = {
            "default": {
                "BACKEND": "channels_glide.GlideChannelLayer",
                "CONFIG": {"hosts": [("localhost", 6379)]},
            },
        }
    """

    brpop_timeout = 5

    extensions = ["groups", "flush"]

    def __init__(
        self,
        hosts: list[Any] | None = None,
        prefix: str = "asgi",
        expiry: int = 60,
        group_expiry: int = 86400,
        capacity: int = 100,
        channel_capacity: Mapping[str, int] | None = None,
        client: GlideClient | None = None,
        **client_kwargs: Any,
    ) -> None:
        super().__init__(expiry=expiry, capacity=capacity, channel_capacity=channel_capacity)
        self.channel_capacity = self.compile_capacities(channel_capacity or {})
        self.group_expiry = group_expiry
        self.prefix = prefix
        assert isinstance(self.prefix, str), "Prefix must be unicode"
        self.hosts: list[Any] = list(hosts) if hosts else [("localhost", 6379)]
        self.client_kwargs = client_kwargs
        self._injected_client: GlideClient | None = client
        self._client: GlideClient | None = None
        # Unique client prefix used in specific ("!") channel names so two
        # layers never hand each other process-local channels.
        self.client_prefix = uuid.uuid4().hex
        # Number of coroutines trying to receive right now
        self.receive_count = 0
        # The receive lock
        self.receive_lock: asyncio.Lock | None = None
        # Event loop they are trying to receive on
        self.receive_event_loop: asyncio.AbstractEventLoop | None = None
        # Buffered messages by process-local channel name
        self.receive_buffer: collections.defaultdict[str, BoundedQueue] = collections.defaultdict(
            functools.partial(BoundedQueue, capacity)
        )
        # Detached channel cleanup tasks
        self.receive_cleaners: list[asyncio.Future[None]] = []
        # Per-channel cleanup locks preventing a receive from moving a message
        # back into the main queue before its cleanup has completed
        self.receive_clean_locks = ChannelLock()

    async def _build_client(self) -> GlideClient:
        addresses: list[NodeAddress] = []
        credentials = None
        for host in self.hosts:
            if isinstance(host, dict):
                if "address" in host and isinstance(host["address"], str):
                    parsed = self._parse_url(host["address"])
                    address, url_credentials = parsed
                    credentials = url_credentials or credentials
                else:
                    address = NodeAddress(
                        str(host.get("host", "localhost")),
                        int(host.get("port", 6379)),
                    )
            elif isinstance(host, str):
                address, url_credentials = self._parse_url(host)
                credentials = url_credentials or credentials
            else:
                h, p = host[0], host[1] if len(host) > 1 else 6379
                address = NodeAddress(str(h), int(p))
            addresses.append(address)
        # valkey-glide requires the async factory: the bare constructor never
        # creates the Rust client and every command fails with a cryptic
        # "initializer for ctype 'void *'" TypeError.
        return await GlideClient.create(  # type: ignore[abstract]
            GlideClientConfiguration(
                addresses=addresses, credentials=credentials, **self.client_kwargs
            )
        )

    @staticmethod
    def _parse_url(url: str) -> tuple[NodeAddress, Any | None]:
        """Parse a ``redis://``/``valkey://`` URL into a node address and,
        when present, server credentials."""
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("redis", "rediss", "valkey", "valkeys", "unix"):
            raise ValueError(f"Unsupported channel layer URL scheme: {url!r}")
        credentials = None
        if parsed.password is not None:
            credentials = ServerCredentials(
                username=parsed.username or None, password=parsed.password
            )
        if parsed.scheme == "unix":
            return NodeAddress(parsed.path), credentials
        return NodeAddress(parsed.hostname or "localhost", parsed.port or 6379), credentials

    async def _get_client(self) -> GlideClient:
        if self._injected_client is not None:
            return self._injected_client
        if self._client is None:
            self._client = await self._build_client()
        return self._client

    ### Channel layer API ###

    async def send(self, channel: str, message: dict[str, Any]) -> None:
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.require_valid_channel_name(channel), "Channel name not valid"
        # Make sure the message does not contain reserved keys
        assert "__asgi_channel__" not in message
        # If it's a process-local channel, strip off local part and stick
        # the full name in the message
        channel_non_local_name = channel
        if "!" in channel:
            message = dict(message.items())
            message["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)
        # Write out the message into an expiring sorted set (avoids big
        # items living in a plain list)
        channel_key = self.prefix + channel_non_local_name
        client = await self._get_client()
        # Discard old messages based on expiry
        await client.zremrangebyscore(
            channel_key,
            ScoreBoundary(0),
            ScoreBoundary(int(time.time()) - int(self.expiry)),
        )
        # Check the size of the set before sending; leaking slightly over
        # capacity is fine.
        if await client.zcount(channel_key, "-inf", "+inf") >= self.get_capacity(channel):
            raise ChannelFull()
        # Add the message then set the key to expire in case it's not consumed
        await client.zadd(channel_key, {self.serialize(message): time.time()})
        await client.expire(channel_key, int(self.expiry))

    def _backup_channel_name(self, channel: str) -> str:
        """Construct the key used as a backup queue for the given channel."""
        return channel + "$inflight"

    async def _brpop_with_clean(self, channel_key: str, timeout: float) -> bytes | None:
        """
        Pop the lowest-scored message and manage the backup processing
        queue. In case of cancellation, make sure the message is not lost.
        """
        backup_queue = self._backup_channel_name(channel_key)
        client = await self._get_client()
        # Cancellation doesn't matter here — the script is atomic and
        # non-destructive; it only restores already-backed-up messages.
        await client.invoke_script(_CLEANUP_SCRIPT, args=[channel_key, backup_queue])
        # The message will be safe in the backup queue from here on.
        result = await client.bzpopmin([channel_key], timeout)
        if result is not None:
            _, member, timestamp = result
            await client.zadd(backup_queue, {member: float(timestamp)})
            member_bytes = (
                member if isinstance(member, bytes) else str(member).encode("utf8")
            )
            return member_bytes
        return None

    async def _clean_receive_backup(self, channel_key: str) -> None:
        """
        Pop the oldest message off the channel backup queue. The result
        isn't interesting as it was already processed.
        """
        client = await self._get_client()
        await client.zpopmin(self._backup_channel_name(channel_key))

    async def receive(self, channel: str) -> dict[str, Any]:
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, the first waiter
        will be given the message when it arrives.
        """
        # Make sure the channel name is valid then get the non-local part
        # and thus its index
        assert self.require_valid_channel_name(channel)
        if "!" in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + "!"), "Wrong client prefix"
            # Enter receiving section
            loop = asyncio.get_running_loop()
            self.receive_count += 1
            try:
                if self.receive_count == 1:
                    # If we're the first coroutine in, create the receive lock!
                    self.receive_lock = asyncio.Lock()
                    self.receive_event_loop = loop
                else:
                    # Otherwise, check our event loop matches
                    if self.receive_event_loop != loop:
                        raise RuntimeError(
                            "Two event loops are trying to receive() on one channel "
                            "layer at once!"
                        )

                # Wait for our message to appear
                receive_lock = self.receive_lock
                assert receive_lock is not None
                message: dict[str, Any] | None = None
                while self.receive_buffer[channel].empty():
                    tasks = [
                        receive_lock.acquire(),
                        self.receive_buffer[channel].get(),
                    ]
                    futures = [asyncio.ensure_future(task) for task in tasks]
                    try:
                        done, pending = await asyncio.wait(
                            futures, return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in pending:
                            # Cancel all pending tasks.
                            task.cancel()
                    except asyncio.CancelledError:
                        # Ensure all tasks are cancelled if we are cancelled.
                        del self.receive_buffer[channel]
                        for task in futures:
                            if not task.cancel():
                                assert task.done()
                                if task.result() is True:
                                    receive_lock.release()
                        raise

                    message = token = exception = None
                    for task in done:
                        try:
                            result: Any = task.result()
                        except BaseException as error:  # noqa: BLE001
                            # Do not propagate immediately, otherwise the lock
                            # may be held and never released.
                            exception = error
                            continue

                        if result is True:
                            token = result
                        else:
                            assert isinstance(result, dict)
                            message = result

                    if message or exception:
                        if token:
                            # We will not be receiving, we already have the message.
                            receive_lock.release()
                        if exception:
                            raise exception
                        break
                    else:
                        assert token

                        # We hold the receive lock, receive and then release it.
                        try:
                            message_channel, received = await self.receive_single(real_channel)
                            if isinstance(message_channel, list):
                                for chan in message_channel:
                                    self.receive_buffer[chan].put_nowait(received)
                            else:
                                self.receive_buffer[message_channel].put_nowait(received)
                            message = None
                        except Exception:
                            del self.receive_buffer[channel]
                            raise
                        finally:
                            receive_lock.release()

                # We know there's a message available, because there couldn't
                # have been any interruption between empty() and here
                if message is None:
                    message = self.receive_buffer[channel].get_nowait()

                if self.receive_buffer[channel].empty():
                    del self.receive_buffer[channel]
                assert message is not None
                return message

            finally:
                self.receive_count -= 1
                # If we were the last out, drop the receive lock
                if self.receive_count == 0:
                    assert self.receive_lock is not None
                    assert not self.receive_lock.locked()
                    self.receive_lock = None
                    self.receive_event_loop = None
        else:
            # Do a plain direct receive
            return (await self.receive_single(channel))[1]

    async def receive_single(self, channel: str) -> tuple[Any, dict[str, Any]]:
        """
        Receive a single message off of the channel and return it.
        """
        # Check channel name
        assert self.require_valid_channel_name(channel, receive=True), "Channel name invalid"
        channel_key = self.prefix + channel
        content: bytes | None = None
        await self.receive_clean_locks.acquire(channel_key)
        try:
            while content is None:
                # Nothing is lost here by cancellations; messages stay in
                # the backup queue.
                content = await self._brpop_with_clean(channel_key, timeout=self.brpop_timeout)

            # Fire off a task to clean the message from its backup queue.
            # Per-channel locking isn't needed: the backup is a queue and
            # ordering doesn't matter — all processed messages must go.
            # NOTE: Duplicate messages will be received eventually if any
            # of these cleaners are cancelled.
            cleaner = asyncio.ensure_future(self._clean_receive_backup(channel_key))
            self.receive_cleaners.append(cleaner)

            def _cleanup_done(cleaner: asyncio.Future[None]) -> None:
                self.receive_cleaners.remove(cleaner)
                self.receive_clean_locks.release(channel_key)

            cleaner.add_done_callback(_cleanup_done)

        except BaseException:
            self.receive_clean_locks.release(channel_key)
            raise

        # Message decode
        message = self.deserialize(content)
        # If there is a full channel name stored in the message, unpack it.
        if "__asgi_channel__" in message:
            channel = message["__asgi_channel__"]
            del message["__asgi_channel__"]
        return channel, message

    async def new_channel(self, prefix: str = "specific") -> str:
        """
        Return a new channel name that can be used by something in our
        process as a specific channel.
        """
        return f"{prefix}.{self.client_prefix}!{uuid.uuid4().hex}"

    ### Flush extension ###

    async def flush(self) -> None:
        """
        Deletes all messages and groups under this layer's prefix.
        """
        # Make sure all channel cleaners have finished before removing
        # keys from under their feet.
        await self.wait_received()
        client = await self._get_client()
        await client.invoke_script(_FLUSH_SCRIPT, args=[self.prefix + "*"])

    async def close(self) -> None:
        """
        Close the GLIDE client created by this layer (a no-op when a client
        was injected).
        """
        await self.wait_received()
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def wait_received(self) -> None:
        """Wait for all channel cleanup functions to finish."""
        if self.receive_cleaners:
            await asyncio.wait(self.receive_cleaners[:])

    ### Groups extension ###

    async def group_add(self, group: str, channel: str) -> None:
        """
        Add the channel name to a group.
        """
        # Check the inputs
        assert self.require_valid_group_name(group), "Group name not valid"
        assert self.require_valid_channel_name(channel), "Channel name not valid"
        group_key = self._group_key(group)
        client = await self._get_client()
        # Add to the group sorted set with creation time as score
        await client.zadd(group_key, {channel: time.time()})
        # Expire with group_expiry, since everything in it at this point is
        # guaranteed to expire before that
        await client.expire(group_key, self.group_expiry)

    async def group_discard(self, group: str, channel: str) -> None:
        """
        Remove the channel from the named group if it is in the group; does
        nothing otherwise (does not error).
        """
        assert self.require_valid_group_name(group), "Group name not valid"
        assert self.require_valid_channel_name(channel), "Channel name not valid"
        client = await self._get_client()
        await client.zrem(self._group_key(group), [channel])

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """
        Send a message to the entire group.
        """
        assert self.require_valid_group_name(group), "Group name not valid"
        key = self._group_key(group)
        client = await self._get_client()
        # Discard old channels based on group_expiry
        await client.zremrangebyscore(
            key, ScoreBoundary(0), ScoreBoundary(int(time.time()) - self.group_expiry)
        )
        channel_names = [
            member.decode("utf8")
            for member in await client.zrange(key, RangeByIndex(0, -1))
        ]
        channel_keys, messages, capacities = self._map_channel_keys(channel_names, message)
        if not channel_keys:
            return
        args = (
            [messages[channel_key] for channel_key in channel_keys]
            + [str(capacities[channel_key]) for channel_key in channel_keys]
            + [str(time.time()), str(self.expiry)]
        )
        over_capacity = await client.invoke_script(
            _GROUP_SEND_SCRIPT, keys=list(channel_keys), args=args
        )
        if over_capacity > 0:
            logger.info(
                "%s of %s channels over capacity in group %s",
                over_capacity,
                len(channel_names),
                group,
            )

    def _map_channel_keys(
        self, channel_names: list[str], message: dict[str, Any]
    ) -> tuple[list[str], dict[str, bytes], dict[str, int]]:
        """
        Map every channel name to its queue key, deduplicating channels that
        share a non-local key: each unique key gets one serialized message
        carrying the list of target channels in ``__asgi_channel__``.
        """
        channel_key_to_message_raw: dict[str, dict[str, Any]] = {}
        channel_key_to_capacity: dict[str, int] = {}
        for channel in channel_names:
            channel_non_local_name = channel
            if "!" in channel:
                channel_non_local_name = self.non_local_name(channel)
            channel_key = self.prefix + channel_non_local_name
            if channel_key not in channel_key_to_message_raw:
                message_copy = dict(message.items())
                message_copy["__asgi_channel__"] = [channel]
                channel_key_to_message_raw[channel_key] = message_copy
                channel_key_to_capacity[channel_key] = self.get_capacity(channel)
            else:
                channel_key_to_message_raw[channel_key]["__asgi_channel__"].append(channel)
        channel_keys = list(channel_key_to_message_raw.keys())
        messages = {k: self.serialize(v) for k, v in channel_key_to_message_raw.items()}
        return channel_keys, messages, channel_key_to_capacity

    def _group_key(self, group: str) -> str:
        """Common function to make the storage key for the group."""
        return f"{self.prefix}:group:{group}"

    ### Serialization ###

    def serialize(self, message: Any) -> bytes:
        """Serialize a message, prefixing it so identical messages stay
        unique members of their sorted set."""
        prefix = uuid.uuid4().hex[:_RANDOM_PREFIX_LENGTH].encode("ascii")
        payload: bytes = msgpack.packb(message, default=_encode_datetime, use_bin_type=True)
        return prefix + payload

    def deserialize(self, message: bytes) -> dict[str, Any]:
        """Strip the uniqueness prefix and decode the message."""
        decoded = msgpack.unpackb(message[_RANDOM_PREFIX_LENGTH:], raw=False)
        assert isinstance(decoded, dict)
        return decoded
