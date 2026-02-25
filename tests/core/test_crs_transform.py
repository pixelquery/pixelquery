"""
Unit tests for CRS transformation helpers in pixelquery.core.api.

Tests _needs_crs_transform, _transform_bounds, _transform_point,
and _transform_geometry using pyproj.
"""

import pytest

from pixelquery.core.api import (
    _needs_crs_transform,
    _transform_bounds,
    _transform_geometry,
    _transform_point,
)


class TestNeedsCrsTransform:
    def test_same_crs(self):
        assert _needs_crs_transform("EPSG:4326", "EPSG:4326") is False

    def test_same_crs_different_format(self):
        # pyproj recognises both as WGS84
        assert _needs_crs_transform("EPSG:4326", "epsg:4326") is False

    def test_different_crs(self):
        assert _needs_crs_transform("EPSG:4326", "EPSG:32652") is True

    def test_none_src(self):
        assert _needs_crs_transform(None, "EPSG:4326") is False

    def test_none_dst(self):
        assert _needs_crs_transform("EPSG:4326", None) is False

    def test_both_none(self):
        assert _needs_crs_transform(None, None) is False

    def test_empty_string(self):
        assert _needs_crs_transform("", "EPSG:4326") is False


class TestTransformBounds:
    def test_identity(self):
        """EPSG:4326 -> EPSG:4326 should return same bounds."""
        bounds = (127.0, 37.5, 127.1, 37.6)
        result = _transform_bounds(bounds, "EPSG:4326", "EPSG:4326")
        assert pytest.approx(result[0], abs=1e-6) == 127.0
        assert pytest.approx(result[1], abs=1e-6) == 37.5
        assert pytest.approx(result[2], abs=1e-6) == 127.1
        assert pytest.approx(result[3], abs=1e-6) == 37.6

    def test_4326_to_utm(self):
        """EPSG:4326 -> EPSG:32652 (UTM 52N): result in meters."""
        bounds = (127.0, 37.5, 127.1, 37.6)
        result = _transform_bounds(bounds, "EPSG:4326", "EPSG:32652")

        # UTM 52N easting should be ~300_000-800_000, northing ~4_000_000+
        assert 300_000 < result[0] < 800_000  # minx (easting)
        assert 4_000_000 < result[1] < 5_000_000  # miny (northing)
        assert result[2] > result[0]  # maxx > minx
        assert result[3] > result[1]  # maxy > miny

    def test_utm_to_4326(self):
        """Round-trip: 4326 -> UTM -> 4326 preserves bounds."""
        original = (127.0, 37.5, 127.1, 37.6)
        utm = _transform_bounds(original, "EPSG:4326", "EPSG:32652")
        back = _transform_bounds(utm, "EPSG:32652", "EPSG:4326")
        assert pytest.approx(back[0], abs=1e-5) == original[0]
        assert pytest.approx(back[1], abs=1e-5) == original[1]
        assert pytest.approx(back[2], abs=1e-5) == original[2]
        assert pytest.approx(back[3], abs=1e-5) == original[3]


class TestTransformPoint:
    def test_identity(self):
        x, y = _transform_point(127.05, 37.55, "EPSG:4326", "EPSG:4326")
        assert pytest.approx(x, abs=1e-6) == 127.05
        assert pytest.approx(y, abs=1e-6) == 37.55

    def test_4326_to_utm(self):
        x, y = _transform_point(127.05, 37.55, "EPSG:4326", "EPSG:32652")
        assert 300_000 < x < 800_000
        assert 4_000_000 < y < 5_000_000

    def test_round_trip(self):
        ux, uy = _transform_point(127.05, 37.55, "EPSG:4326", "EPSG:32652")
        bx, by = _transform_point(ux, uy, "EPSG:32652", "EPSG:4326")
        assert pytest.approx(bx, abs=1e-6) == 127.05
        assert pytest.approx(by, abs=1e-6) == 37.55


class TestTransformGeometry:
    def test_polygon_dict(self):
        """Transform a GeoJSON dict polygon."""
        geojson = {
            "type": "Polygon",
            "coordinates": [
                [[127.0, 37.5], [127.1, 37.5], [127.1, 37.6], [127.0, 37.6], [127.0, 37.5]]
            ],
        }
        result = _transform_geometry(geojson, "EPSG:4326", "EPSG:32652")
        # Result should be a shapely geometry in UTM coords
        bx, by, _, _ = result.bounds
        assert 300_000 < bx < 800_000
        assert 4_000_000 < by < 5_000_000

    def test_shapely_input(self):
        """Transform a shapely geometry object directly."""
        from shapely.geometry import box

        geom = box(127.0, 37.5, 127.1, 37.6)
        result = _transform_geometry(geom, "EPSG:4326", "EPSG:32652")
        bx, _, _, _ = result.bounds
        assert 300_000 < bx < 800_000

    def test_round_trip(self):
        """Round-trip preserves geometry bounds."""
        from shapely.geometry import box

        original = box(127.0, 37.5, 127.1, 37.6)
        utm = _transform_geometry(original, "EPSG:4326", "EPSG:32652")
        back = _transform_geometry(utm, "EPSG:32652", "EPSG:4326")
        bx, by, bxx, byy = back.bounds
        assert pytest.approx(bx, abs=1e-5) == 127.0
        assert pytest.approx(by, abs=1e-5) == 37.5
        assert pytest.approx(bxx, abs=1e-5) == 127.1
        assert pytest.approx(byy, abs=1e-5) == 37.6
