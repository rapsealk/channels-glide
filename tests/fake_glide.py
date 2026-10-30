"""In-memory stand-in for ``glide.GlideClient``.

Implements the exact command surface ``channels_glide`` uses, including
Python emulations of the layer's three Lua scripts (cleanup restore,
group-send scatter, prefix flush). Unit tests run against this fake so no
Valkey server is required; ``test_integration.py`` covers the real thing.
"""

from __future__ import annotations

import collections
import fnmatch
from collections.abc import Mapping
from typing import Any


def _script_text(script: Any) -> str:
    for attr in ("code", "_code", "script", "_script", "source"):
        text = getattr(script, attr, None)
        if isinstance(text, str):
            return text
    return str(script)


class ScoreBoundary:
    """Mirror of glide's ScoreBoundary for bound parsing."""

    def __init__(self, value: float, is_inclusive: bool = True) -> None:
        self.value = value
        self.is_inclusive = is_inclusive


def _bound_value(bound: Any) -> float | None:
    """Translate a min/max bound (ScoreBoundary, inf string, or float)."""
    if isinstance(bound, str):
        if bound in ("-inf", "+inf"):
            return None  # caller handles infinities
        return float(bound)
    if isinstance(bound, ScoreBoundary):
        return float(bound.value)
    return float(bound)


def _in_range(score: float, min_bound: Any, max_bound: bool | float | object) -> bool:
    def _check(bound: Any, low: bool) -> bool:
        raw = getattr(bound, "value", bound)
        if isinstance(raw, dict) and "score_arg" in raw:
            # glide's InfBound enum (+/- infinity always contains)
            return raw["score_arg"] in ("-inf", "+inf")
        if isinstance(bound, str):
            if bound in ("-inf", "+inf"):
                return True
            value, inclusive = float(bound), True
        else:
            # Duck-type glide's ScoreBoundary (the fake must not isinstance
            # against its own mirror class).
            value = float(raw)
            inclusive = getattr(bound, "is_inclusive", True)
        if low:
            return score >= value if inclusive else score > value
        return score <= value if inclusive else score < value

    return _check(min_bound, low=True) and _check(max_bound, low=False)


class FakeGlideClient:
    """Minimal async fake of the GLIDE standalone client."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[bytes, float]] = collections.defaultdict(dict)
        self.expire_calls: list[tuple[str, int]] = []
        self.closed = False

    # --- sorted-set commands ---

    async def zadd(self, key: str, members_scores: Mapping[Any, float]) -> int:
        zset = self.zsets[key]
        for member, score in members_scores.items():
            zset[self._as_bytes(member)] = float(score)
        return len(members_scores)

    async def zcount(self, key: str, min_score: Any, max_score: Any) -> int:
        zset = self.zsets.get(key, {})
        return sum(1 for score in zset.values() if _in_range(score, min_score, max_score))

    async def zremrangebyscore(self, key: str, min_score: Any, max_score: Any) -> int:
        zset = self.zsets.get(key, {})
        victims = [
            member
            for member, score in zset.items()
            if _in_range(score, min_score, max_score)
        ]
        for member in victims:
            del zset[member]
        return len(victims)

    async def zrange(self, key: str, range_query: Any, reverse: bool = False) -> list[bytes]:
        zset = self.zsets.get(key, {})
        ordered = sorted(zset.items(), key=lambda item: item[1])
        if reverse:
            ordered = list(reversed(ordered))
        start, end = range_query.start, range_query.end
        sliced = ordered[start : end + 1 if end >= 0 else None]
        return [member for member, _ in sliced]

    async def zrem(self, key: str, members: list[Any]) -> int:
        zset = self.zsets.get(key, {})
        removed = 0
        for member in members:
            if self._as_bytes(member) in zset:
                del zset[self._as_bytes(member)]
                removed += 1
        return removed

    async def bzpopmin(self, keys: list[str], timeout: float) -> list[Any] | None:
        for key in keys:
            zset = self.zsets.get(key, {})
            if zset:
                member, score = min(zset.items(), key=lambda item: item[1])
                del zset[member]
                return [key.encode("utf8"), member, score]
        return None

    async def zpopmin(self, key: str, count: int | None = None) -> dict[bytes, float]:
        zset = self.zsets.get(key, {})
        ordered = sorted(zset.items(), key=lambda item: item[1])
        if count is not None:
            ordered = ordered[:count]
        else:
            ordered = ordered[:1]
        popped: dict[bytes, float] = {}
        for member, score in ordered:
            del zset[member]
            popped[member] = score
        return popped

    async def expire(self, key: str, seconds: int) -> bool:
        self.expire_calls.append((key, seconds))
        return True

    # --- scripting ---

    async def invoke_script(
        self,
        script: Any,
        keys: list[str] | None = None,
        args: list[Any] | None = None,
    ) -> Any:
        # glide's Script object exposes only its hash, so identify the layer's
        # scripts by identity or hash against the module constants.
        import channels_glide.glide as layer_module

        which = None
        for name, kind in (
            ("_CLEANUP_SCRIPT", "cleanup"),
            ("_GROUP_SEND_SCRIPT", "group_send"),
            ("_FLUSH_SCRIPT", "flush"),
        ):
            expected = getattr(layer_module, name)
            if script is expected or self._same_hash(script, expected):
                which = kind
                break
        text = _script_text(script)
        if which is None and text is not None:
            if "ZRANGE" in text and "DEL" in text:
                which = "cleanup"
            elif "ZCOUNT" in text and "over_capacity" in text:
                which = "group_send"
            elif "keys" in text and "del" in text:
                which = "flush"
        if which is None:
            raise AssertionError(f"FakeGlideClient does not recognize script: {script!r}")
        keys = keys or []
        args = args or []
        if which == "cleanup":
            # Cleanup script: restore ARGV[2] (backup) into ARGV[1] (main).
            main, backup = str(args[0]), str(args[1])
            backed = sorted(self.zsets.get(backup, {}).items(), key=lambda item: item[1])
            for member, score in reversed(backed):
                self.zsets[main][member] = score
            self.zsets.pop(backup, None)
            return None
        if which == "group_send":
            # Group-send script: KEYS = queues, ARGV = msgs..., caps..., time, expiry.
            over_capacity = 0
            current_time = float(args[-2])
            expiry = int(args[-1])
            for i, key in enumerate(keys):
                zset = self.zsets.get(key, {})
                capacity = int(args[i + len(keys)])
                if len(zset) < capacity:
                    message = args[i]
                    self.zsets[key][
                        message if isinstance(message, bytes) else str(message).encode("utf8")
                    ] = current_time
                    self.expire_calls.append((key, expiry))
                else:
                    over_capacity += 1
            return over_capacity
        # Flush script: delete every key matching ARGV[1] glob.
        pattern = str(args[0])
        victims = [key for key in self.zsets if fnmatch.fnmatchcase(key, pattern)]
        for key in victims:
            del self.zsets[key]
        return None

    @staticmethod
    def _same_hash(left: Any, right: Any) -> bool:
        get_hash = getattr(left, "get_hash", None)
        other = getattr(right, "get_hash", None)
        if get_hash is None or other is None:
            return False
        try:
            return bool(get_hash() == other())
        except Exception:
            return False

    # --- lifecycle ---

    async def close(self) -> None:
        self.closed = True

    @staticmethod
    def _as_bytes(member: Any) -> bytes:
        return member if isinstance(member, bytes) else str(member).encode("utf8")


class RangeByIndex:
    """Mirror of glide's RangeByIndex for the fake's zrange."""

    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end
