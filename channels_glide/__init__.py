"""channels-glide — Django Channels channel layer backed by Valkey GLIDE."""

from .glide import GlideChannelLayer

__version__ = "0.1.0"

__all__ = ["GlideChannelLayer", "__version__"]
