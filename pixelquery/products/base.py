"""
Product Profile and Band Information

Defines satellite/sensor product specifications.
Users can create custom profiles via dict, YAML, or direct instantiation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Band:
    """
    Sensor band metadata.

    Args:
        standard_name: Standardized name (e.g., "red", "nir")
        wavelength: Center wavelength in nanometers
        resolution: Spatial resolution in meters
        native_name: Sensor-specific name (e.g., "B04"). Defaults to standard_name.
        bandwidth: Spectral bandwidth in nanometers (optional)

    Examples:
        >>> Band("red", 665.0, 10.0)
        >>> Band("nir", 842.0, 10.0, native_name="B08", bandwidth=115.0)
    """

    standard_name: str
    wavelength: float
    resolution: float
    native_name: str | None = None
    bandwidth: float | None = None

    def __post_init__(self):
        if self.native_name is None:
            object.__setattr__(self, "native_name", self.standard_name)


@dataclass
class ProductProfile:
    """
    Sensor product specification.

    Can be created directly, from a dict, or from a JSON/YAML file.

    Attributes:
        product_id: Unique identifier (e.g., "sentinel2_l2a", "dji_mavic3m")
        bands: Dict mapping standard band names to Band objects
        provider: Data provider (e.g., "ESA", "DJI")
        sensor: Sensor name (e.g., "MSI", "Mavic3M-MS")
        native_resolution: Primary spatial resolution in meters
        scale_factor: DN to reflectance conversion factor
        offset: Additive offset for conversion
        nodata: No-data pixel value
        description: Human-readable description

    Examples:
        >>> # Direct instantiation
        >>> profile = ProductProfile(
        ...     product_id="dji_mavic3m",
        ...     bands={
        ...         "green": Band("green", 560, 0.05),
        ...         "red": Band("red", 650, 0.05),
        ...         "red_edge": Band("red_edge", 730, 0.05),
        ...         "nir": Band("nir", 860, 0.05),
        ...     },
        ...     provider="DJI",
        ...     sensor="Mavic3M-MS",
        ...     native_resolution=0.05,
        ... )
        >>>
        >>> # From dict
        >>> profile = ProductProfile.from_dict({
        ...     "product_id": "custom_sensor",
        ...     "bands": {"red": {"wavelength": 650, "resolution": 3.0}},
        ... })
        >>>
        >>> # Register for global lookup
        >>> register_profile(profile)
        >>> get_profile("dji_mavic3m")
    """

    product_id: str
    bands: dict[str, Band] = field(default_factory=dict)
    provider: str = ""
    sensor: str = ""
    native_resolution: float = 0.0
    scale_factor: float = 1.0
    offset: float = 0.0
    nodata: int = 0
    description: str = ""

    @property
    def band_names(self) -> list[str]:
        """Return ordered list of standard band names."""
        return list(self.bands.keys())

    @property
    def band_count(self) -> int:
        return len(self.bands)

    def get_band(self, name: str) -> Band:
        """Get band by standard name or native name."""
        if name in self.bands:
            return self.bands[name]
        for b in self.bands.values():
            if b.native_name == name:
                return b
        raise KeyError(f"Band '{name}' not found in {self.product_id}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProductProfile:
        """Create profile from a dictionary.

        Band entries can be:
          - Band objects (passthrough)
          - dicts with {wavelength, resolution, ...}
          - simple [wavelength, resolution] lists

        Examples:
            >>> ProductProfile.from_dict({
            ...     "product_id": "my_sensor",
            ...     "bands": {
            ...         "red": {"wavelength": 650, "resolution": 3.0},
            ...         "nir": [860, 3.0],
            ...     },
            ...     "nodata": -999,
            ... })
        """
        raw_bands = data.pop("bands", {})
        bands: dict[str, Band] = {}

        for name, spec in raw_bands.items():
            if isinstance(spec, Band):
                bands[name] = spec
            elif isinstance(spec, dict):
                bands[name] = Band(
                    standard_name=spec.get("standard_name", name),
                    wavelength=float(spec.get("wavelength", 0)),
                    resolution=float(spec.get("resolution", 0)),
                    native_name=spec.get("native_name"),
                    bandwidth=spec.get("bandwidth"),
                )
            elif isinstance(spec, (list, tuple)) and len(spec) >= 2:
                bands[name] = Band(
                    standard_name=name,
                    wavelength=float(spec[0]),
                    resolution=float(spec[1]),
                    native_name=spec[2] if len(spec) > 2 else None,
                    bandwidth=spec[3] if len(spec) > 3 else None,
                )
            else:
                raise ValueError(
                    f"Invalid band spec for '{name}': {spec}. "
                    "Expected Band, dict, or [wavelength, resolution]."
                )

        return cls(bands=bands, **data)

    @classmethod
    def from_json(cls, path: str | Path) -> ProductProfile:
        """Load profile from a JSON file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict (JSON-safe)."""
        return {
            "product_id": self.product_id,
            "provider": self.provider,
            "sensor": self.sensor,
            "native_resolution": self.native_resolution,
            "scale_factor": self.scale_factor,
            "offset": self.offset,
            "nodata": self.nodata,
            "description": self.description,
            "bands": {
                name: {
                    "standard_name": b.standard_name,
                    "wavelength": b.wavelength,
                    "resolution": b.resolution,
                    "native_name": b.native_name,
                    "bandwidth": b.bandwidth,
                }
                for name, b in self.bands.items()
            },
        }

    def to_json(self, path: str | Path) -> None:
        """Save profile to a JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    def __repr__(self) -> str:
        bands_str = ", ".join(self.band_names)
        return (
            f"<ProductProfile '{self.product_id}'>\n"
            f"  Provider: {self.provider or '(custom)'}\n"
            f"  Sensor: {self.sensor or '(custom)'}\n"
            f"  Resolution: {self.native_resolution}m\n"
            f"  Bands ({self.band_count}): {bands_str}\n"
            f"  Scale: DN * {self.scale_factor} + {self.offset}\n"
            f"  Nodata: {self.nodata}"
        )


# ── Global Profile Registry ──

_REGISTRY: dict[str, ProductProfile] = {}


def register_profile(profile: ProductProfile) -> None:
    """Register a profile for global lookup by product_id."""
    _REGISTRY[profile.product_id] = profile
    logger.info("Registered profile: %s", profile.product_id)


def get_profile(product_id: str) -> ProductProfile:
    """Get a registered profile by product_id.

    Raises:
        KeyError: If profile not found
    """
    if product_id not in _REGISTRY:
        raise KeyError(
            f"Profile '{product_id}' not found. "
            f"Available: {list(_REGISTRY.keys())}. "
            f"Use register_profile() to add custom profiles."
        )
    return _REGISTRY[product_id]


def list_profiles() -> list[str]:
    """List all registered profile IDs."""
    return list(_REGISTRY.keys())


# ── BandInfo Protocol (backward compatibility) ──

BandInfo = Band  # alias for old imports
