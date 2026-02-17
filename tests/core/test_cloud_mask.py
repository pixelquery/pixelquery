"""Tests for CloudMask and cloud masking pipeline."""

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest


class TestCloudMask:
    def test_create(self):
        from pixelquery.catalog.product_profile import CloudMask

        cm = CloudMask(band_index=5, clear_values=[0], file_suffix="_udm2")
        assert cm.band_index == 5
        assert cm.clear_values == [0]
        assert cm.file_suffix == "_udm2"

    def test_to_dict_from_dict(self):
        from pixelquery.catalog.product_profile import CloudMask

        cm = CloudMask(band_index=0, clear_values=[4, 5, 6, 7], file_suffix="_SCL")
        d = cm.to_dict()
        cm2 = CloudMask.from_dict(d)
        assert cm2.band_index == 0
        assert cm2.clear_values == [4, 5, 6, 7]
        assert cm2.file_suffix == "_SCL"

    def test_default_clear_values(self):
        from pixelquery.catalog.product_profile import CloudMask

        cm = CloudMask(band_index=5)
        assert cm.clear_values == [0]


class TestProductProfileWithCloudMask:
    def test_builtin_planet_has_cloud_mask(self):
        from pixelquery.catalog.product_profile import BUILTIN_PROFILES

        planet = BUILTIN_PROFILES["planet_sr"]
        assert planet.cloud_mask is not None
        assert planet.cloud_mask.band_index == 5
        assert planet.cloud_mask.clear_values == [0]
        assert planet.cloud_mask.file_suffix == "_udm2"

    def test_builtin_sentinel2_has_cloud_mask(self):
        from pixelquery.catalog.product_profile import BUILTIN_PROFILES

        s2 = BUILTIN_PROFILES["sentinel2_l2a"]
        assert s2.cloud_mask is not None
        assert 4 in s2.cloud_mask.clear_values  # vegetation
        assert 5 in s2.cloud_mask.clear_values  # bare soil

    def test_profile_to_dict_includes_cloud_mask(self):
        from pixelquery.catalog.product_profile import BUILTIN_PROFILES, ProductProfile

        planet = BUILTIN_PROFILES["planet_sr"]
        d = planet.to_dict()
        assert "cloud_mask" in d
        assert d["cloud_mask"]["band_index"] == 5

        # Round-trip
        restored = ProductProfile.from_dict(d)
        assert restored.cloud_mask is not None
        assert restored.cloud_mask.band_index == 5

    def test_profile_without_cloud_mask(self):
        from pixelquery.catalog.product_profile import ProductProfile

        p = ProductProfile(
            product_id="test",
            bands={"r": 0},
            resolution=1.0,
        )
        assert p.cloud_mask is None
        d = p.to_dict()
        assert "cloud_mask" not in d

        restored = ProductProfile.from_dict(d)
        assert restored.cloud_mask is None


class TestMaskFileMatching:
    def test_match_by_date(self, tmp_path):
        from pixelquery.io.auto_ingest import _match_mask_file

        # Create data file
        data_file = tmp_path / "data" / "2024-01-15_scene_sr.tif"
        data_file.parent.mkdir()
        data_file.touch()

        # Create mask files
        mask_dir = tmp_path / "masks"
        mask_dir.mkdir()
        (mask_dir / "2024-01-15_scene_udm2.tif").touch()
        (mask_dir / "2024-06-15_scene_udm2.tif").touch()

        result = _match_mask_file(data_file, mask_dir)
        assert result is not None
        assert "2024-01-15" in result.name

    def test_no_match(self, tmp_path):
        from pixelquery.io.auto_ingest import _match_mask_file

        data_file = tmp_path / "2024-01-15_sr.tif"
        data_file.touch()

        mask_dir = tmp_path / "masks"
        mask_dir.mkdir()
        (mask_dir / "2024-06-15_udm2.tif").touch()

        result = _match_mask_file(data_file, mask_dir)
        assert result is None


class TestCloudMaskIngest:
    def test_ingest_with_mask(self, tmp_path):
        """Test ingesting COGs with cloud mask files."""
        try:
            import rasterio
            from rasterio.transform import from_bounds

            from pixelquery.io.auto_ingest import ingest
            from pixelquery.sample_data import create_sample_data
        except ImportError:
            pytest.skip("Dependencies not installed")

        # Create sample COGs
        cog_dir = create_sample_data(str(tmp_path / "cogs"))

        # Create matching mask files (simple single-band masks)
        mask_dir = tmp_path / "masks"
        mask_dir.mkdir()

        for cog_file in sorted(Path(cog_dir).glob("*.tif")):
            mask_file = mask_dir / cog_file.name.replace("_sr.", "_udm2.")
            # Create a simple 8-band mask (like UDM2)
            with rasterio.open(cog_file) as src:
                profile = src.profile.copy()
                profile.update(count=8, dtype="uint8")
                with rasterio.open(str(mask_file), "w", **profile) as dst:
                    for b in range(1, 9):
                        # Band 6 (index 5) is cloud: all zeros = clear
                        dst.write(np.zeros((src.height, src.width), dtype=np.uint8), b)

        warehouse = str(tmp_path / "warehouse")

        result = ingest(
            cog_dir,
            warehouse=warehouse,
            band_names=["blue", "green", "red", "nir"],
            product_id="planet_sr",
            mask_path=str(mask_dir),
        )

        assert result.scene_count == 3
        assert len(result.errors) == 0
