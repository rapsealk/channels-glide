# channels-glide

Django Channels channel layer backed by [Valkey GLIDE](https://github.com/valkey-io/valkey-glide).

A port of `channels_redis.core.RedisChannelLayer` semantics onto the Valkey GLIDE
client: expiring sorted-set message queues, backup queues for crash-safe
receives, and group support — driven by a single GLIDE client instead of a
sharded connection ring.

## Why

`channels_redis` is the only Django-maintained channel layer, and there is no
GLIDE-based equivalent. Teams standardizing on Valkey GLIDE (the client the
[Valkey](https://github.com/valkey-io) project and AWS maintain, and the one
upstream Backend.AI adopted) have had no drop-in layer. This package fills
that gap, typed from day one (`py.typed`).

## Install

```bash
pip install channels-glide
```

## Usage

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_glide.GlideChannelLayer",
        "CONFIG": {
            "hosts": [("localhost", 6379)],
        },
    },
}
```

`GlideChannelLayer` accepts the same tuning knobs as `channels_redis`:
`prefix`, `expiry`, `group_expiry`, `capacity`, and `channel_capacity`.
Pass a ready `client=GlideClient(...)` to reuse an existing GLIDE client;
otherwise one is built lazily from `hosts` (plus any extra
`GlideClientConfiguration` kwargs).

## Semantics

- **Queues** are Valkey sorted sets scored by receive time; `send` discards
  aged-out messages, enforces per-channel `capacity` (raising
  `ChannelFull`), and expires the key if unconsumed.
- **Receives** pop into an `$inflight` backup queue and clean it up after
  processing, so a cancelled consumer never loses a message (same protocol
  as `channels_redis`).
- **Groups** are sorted sets scored by membership time, expired after
  `group_expiry`; `group_send` scatters atomically via Lua, skipping
  channels over capacity.
- **Local channels** (`!`-suffixed) get the full multi-waiter buffer with a
  per-layer `client_prefix`, exactly like `channels_redis`.

## Differences from channels_redis

- Single GLIDE client (standalone mode) — GLIDE handles topology internally;
  multi-node sharding via `GlideClusterClient` is on the roadmap.
- `flush()` deletes keys under the prefix but does not close the client;
  call `await layer.close()` explicitly.
- Ships `py.typed` (both `django-redis` and `channels-redis` are untyped).

## Status

Alpha. The layer interface is stable; the Lua protocols are ports of
`channels_redis` and integration-tested against Valkey in CI. Unit tests run
against an in-memory fake so no server is needed.

## Development

```bash
uv venv && uv pip install -e '.[test]'   # or pip install -e '.[test]'
pytest                                    # unit tests (no server needed)
VALKEY_URL=redis://localhost:6379/0 pytest tests/test_integration.py
ruff check .
mypy channels_glide
```

## License

Apache-2.0
