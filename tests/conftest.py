"""Shared fixtures: a layer wired to an in-memory fake GLIDE client."""

from __future__ import annotations

import pytest
from fake_glide import FakeGlideClient

from channels_glide import GlideChannelLayer


@pytest.fixture()
def fake_client() -> FakeGlideClient:
    return FakeGlideClient()


@pytest.fixture()
def layer(fake_client: FakeGlideClient) -> GlideChannelLayer:
    return GlideChannelLayer(prefix="test", client=fake_client)  # type: ignore[arg-type]
