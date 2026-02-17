"""
Codec compatibility layer for Icechunk/VirtualTIFF.

Fixes imagecodecs numcodecs bug where from_config() passes
'name'/'configuration' keys that __init__() doesn't accept.
Applied once at package import time.
"""

import inspect
import logging

logger = logging.getLogger(__name__)
_PATCHED = False


def patch_imagecodecs():
    """
    Monkeypatch all imagecodecs numcodecs to strip extra keys from from_config().

    Safe to call multiple times; only patches once.
    """
    global _PATCHED
    if _PATCHED:
        return

    try:
        import imagecodecs.numcodecs as ic_numcodecs
    except ImportError:
        logger.debug("imagecodecs.numcodecs not available, skipping codec patch")
        return

    patched_count = 0
    for name in dir(ic_numcodecs):
        cls = getattr(ic_numcodecs, name)
        if not isinstance(cls, type) or not hasattr(cls, "from_config"):
            continue

        try:
            init_params = set(inspect.signature(cls.__init__).parameters.keys()) - {"self"}  # type: ignore[misc]
        except (ValueError, TypeError):
            init_params = set()

        def _make_patched(valid_params):
            @classmethod  # type: ignore[misc]
            def patched_from_config(klass, config):
                config = dict(config)
                for key in ("id", "name", "configuration"):
                    config.pop(key, None)
                if valid_params:
                    config = {k: v for k, v in config.items() if k in valid_params}
                return klass(**config)

            return patched_from_config

        cls.from_config = _make_patched(init_params)
        patched_count += 1

    # Register virtual_tiff codecs under their short names for zarr v3.
    # Entry points register them as "virtual_tiff.ChunkyCodec" but zarr
    # metadata stores just "ChunkyCodec".
    try:
        import zarr.registry
        from virtual_tiff.codecs import ChunkyCodec, HorizontalDeltaCodec

        for codec_cls in (ChunkyCodec, HorizontalDeltaCodec):
            codec_name = codec_cls.__name__
            try:
                zarr.registry.get_codec_class(codec_name)
            except KeyError:
                zarr.registry.register_codec(codec_name, codec_cls)
                patched_count += 1
    except ImportError:
        pass

    _PATCHED = True
    if patched_count:
        logger.debug("Patched %d codecs for imagecodecs/virtual_tiff compatibility", patched_count)
