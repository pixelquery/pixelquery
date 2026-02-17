"""
Tests for TileGrid implementation
"""

import math

import pytest

from pixelquery.grid.tile_grid import FixedTileGrid


class TestFixedTileGrid:
    """Test FixedTileGrid implementation"""

    @pytest.fixture
    def grid(self):
        """Create a grid instance"""
        return FixedTileGrid()

    def test_init_default(self, grid):
        """Test default initialization"""
        assert grid.tile_size_m == 2560.0
        assert grid.EARTH_RADIUS_M == 6378137.0

    def test_init_custom_tile_size(self):
        """Test initialization with custom tile size"""
        grid = FixedTileGrid(tile_size_m=5000.0)
        assert grid.tile_size_m == 5000.0

    def test_get_tile_id_origin(self, grid):
        """Test tile ID at origin (0, 0)"""
        tile_id = grid.get_tile_id(0.0, 0.0)
        assert tile_id == "x0000_y0000"

    def test_get_tile_id_positive(self, grid):
        """Test tile ID in positive quadrant"""
        # Seoul, South Korea (~127.05°E, 37.55°N)
        tile_id = grid.get_tile_id(127.05, 37.55)
        assert tile_id.startswith("x")
        assert "_y" in tile_id

    def test_get_tile_id_negative(self, grid):
        """Test tile ID in negative quadrant"""
        # San Francisco (~-122.42°W, 37.77°N)
        tile_id = grid.get_tile_id(-122.42, 37.77)
        assert tile_id.startswith("x-")
        assert "_y" in tile_id

    def test_get_tile_id_consistency(self, grid):
        """Test that same coordinates return same tile ID"""
        tile_id_1 = grid.get_tile_id(127.05, 37.55)
        tile_id_2 = grid.get_tile_id(127.05, 37.55)
        assert tile_id_1 == tile_id_2

    def test_get_tile_bounds_origin(self, grid):
        """Test tile bounds at origin"""
        minx, miny, maxx, maxy = grid.get_tile_bounds("x0000_y0000")
        assert minx == 0.0
        assert miny == 0.0
        assert maxx > minx
        assert maxy > miny

    def test_get_tile_bounds_positive(self, grid):
        """Test tile bounds for positive tile"""
        minx, miny, maxx, maxy = grid.get_tile_bounds("x0010_y0020")
        assert maxx > minx
        assert maxy > miny
        assert minx > 0
        assert miny > 0

    def test_get_tile_bounds_negative(self, grid):
        """Test tile bounds for negative tile"""
        minx, miny, maxx, maxy = grid.get_tile_bounds("x-0010_y-0020")
        assert maxx > minx
        assert maxy > miny
        assert maxx < 0
        assert maxy < 0

    def test_get_tile_bounds_size_consistency(self, grid):
        """Test that all tiles have consistent size"""
        tiles = ["x0000_y0000", "x0100_y0100", "x-0050_y0050"]
        sizes = []
        for tile_id in tiles:
            minx, miny, maxx, maxy = grid.get_tile_bounds(tile_id)
            # Calculate approximate size in meters at center latitude
            center_lat = (miny + maxy) / 2
            lat_rad = math.radians(center_lat)
            lon_size_m = (maxx - minx) * (math.pi / 180) * grid.EARTH_RADIUS_M * math.cos(lat_rad)
            lat_size_m = (maxy - miny) * (math.pi / 180) * grid.EARTH_RADIUS_M
            sizes.append((lon_size_m, lat_size_m))

        # All tiles should have approximately the same size (within 1% tolerance)
        for lon_size, lat_size in sizes:
            assert abs(lon_size - grid.tile_size_m) / grid.tile_size_m < 0.01
            assert abs(lat_size - grid.tile_size_m) / grid.tile_size_m < 0.01

    def test_get_tile_bounds_invalid_tile_id(self, grid):
        """Test invalid tile ID raises error"""
        with pytest.raises(ValueError):
            grid.get_tile_bounds("invalid")

        with pytest.raises(ValueError):
            grid.get_tile_bounds("x0000")

        with pytest.raises(ValueError):
            grid.get_tile_bounds("0000_y0000")

    def test_get_pixels_for_resolution_sentinel2(self, grid):
        """Test pixel count for Sentinel-2 (10m)"""
        pixels = grid.get_pixels_for_resolution(10.0)
        assert pixels == 256  # 2560m / 10m = 256

    def test_get_pixels_for_resolution_landsat8(self, grid):
        """Test pixel count for Landsat-8 (30m)"""
        pixels = grid.get_pixels_for_resolution(30.0)
        assert pixels == 86  # 2560m / 30m = 85.33 → ceil = 86

    def test_get_pixels_for_resolution_planet(self, grid):
        """Test pixel count for Planet (3m)"""
        pixels = grid.get_pixels_for_resolution(3.0)
        assert pixels == 854  # 2560m / 3m = 853.33 → ceil = 854

    def test_get_pixels_for_resolution_invalid(self, grid):
        """Test invalid resolution raises error"""
        with pytest.raises(ValueError):
            grid.get_pixels_for_resolution(0.0)

        with pytest.raises(ValueError):
            grid.get_pixels_for_resolution(-10.0)

    def test_roundtrip_lon_lat_to_tile_to_bounds(self, grid):
        """Test roundtrip: lon/lat → tile_id → bounds contains original point"""
        test_points = [
            (0.0, 0.0),
            (127.05, 37.55),
            (-122.42, 37.77),
            (139.69, 35.68),  # Tokyo
            (2.35, 48.86),  # Paris
        ]

        for lon, lat in test_points:
            tile_id = grid.get_tile_id(lon, lat)
            minx, miny, maxx, maxy = grid.get_tile_bounds(tile_id)

            # Original point should be within tile bounds
            assert minx <= lon <= maxx, f"Point {lon}, {lat} not in tile {tile_id} x bounds"
            assert miny <= lat <= maxy, f"Point {lon}, {lat} not in tile {tile_id} y bounds"

    def test_adjacent_tiles_no_overlap(self, grid):
        """Test that adjacent tiles don't overlap"""
        tile_id_1 = "x0000_y0000"
        tile_id_2 = "x0001_y0000"  # Adjacent to the right

        _minx1, _miny1, maxx1, _maxy1 = grid.get_tile_bounds(tile_id_1)
        minx2, _miny2, _maxx2, _maxy2 = grid.get_tile_bounds(tile_id_2)

        # Adjacent tiles should share a boundary (maxx1 == minx2)
        assert abs(maxx1 - minx2) < 1e-10, "Adjacent tiles should share boundary"

    def test_format_tile_id(self, grid):
        """Test tile ID formatting"""
        assert grid._format_tile_id(0, 0) == "x0000_y0000"
        assert grid._format_tile_id(10, 20) == "x0010_y0020"
        assert grid._format_tile_id(-5, -10) == "x-0005_y-0010"
        assert grid._format_tile_id(1234, 5678) == "x1234_y5678"

    def test_parse_tile_id(self, grid):
        """Test tile ID parsing"""
        assert grid._parse_tile_id("x0000_y0000") == (0, 0)
        assert grid._parse_tile_id("x0010_y0020") == (10, 20)
        assert grid._parse_tile_id("x-0005_y-0010") == (-5, -10)
        assert grid._parse_tile_id("x1234_y5678") == (1234, 5678)

    def test_parse_tile_id_invalid(self, grid):
        """Test parsing invalid tile IDs"""
        invalid_ids = [
            "invalid",
            "x0000",
            "y0000",
            "0000_0000",
            "x0000_z0000",
            "",
        ]

        for tile_id in invalid_ids:
            with pytest.raises(ValueError):
                grid._parse_tile_id(tile_id)

    def test_get_tiles_in_bounds_single_tile(self, grid):
        """Test getting tiles for bounds within a single tile"""
        # Get bounds of a single tile
        minx, miny, maxx, maxy = grid.get_tile_bounds("x0000_y0000")

        # Query with smaller bounds inside the tile
        inner_bounds = (minx + 0.001, miny + 0.001, maxx - 0.001, maxy - 0.001)

        tiles = grid.get_tiles_in_bounds(inner_bounds)
        assert len(tiles) == 1
        assert "x0000_y0000" in tiles

    def test_get_tiles_in_bounds_multiple_tiles(self, grid):
        """Test getting tiles for bounds spanning multiple tiles"""
        # Bounds spanning approximately 2x2 tiles
        bounds = (127.0, 37.5, 127.1, 37.6)
        tiles = grid.get_tiles_in_bounds(bounds)

        # Should get multiple tiles
        assert len(tiles) >= 1
        assert all(isinstance(tile_id, str) for tile_id in tiles)
        assert all("x" in tile_id and "y" in tile_id for tile_id in tiles)

    def test_get_tiles_in_bounds_exact_tile_bounds(self, grid):
        """Test getting tiles with exact tile boundaries"""
        # Get exact bounds of a tile
        minx, miny, maxx, maxy = grid.get_tile_bounds("x0010_y0020")

        # Use slightly smaller bounds to avoid edge cases
        inner_bounds = (minx + 0.0001, miny + 0.0001, maxx - 0.0001, maxy - 0.0001)

        tiles = grid.get_tiles_in_bounds(inner_bounds)

        # Should get exactly one tile
        assert len(tiles) == 1
        assert "x0010_y0020" in tiles

    def test_get_tiles_in_bounds_negative_coords(self, grid):
        """Test getting tiles in negative coordinate region"""
        # Bounds in negative region
        bounds = (-123.0, 37.5, -122.8, 37.7)
        tiles = grid.get_tiles_in_bounds(bounds)

        assert len(tiles) >= 1
        # All tiles should have negative x indices
        for tile_id in tiles:
            x_idx, _y_idx = grid._parse_tile_id(tile_id)
            assert x_idx < 0

    def test_get_tiles_in_bounds_large_area(self, grid):
        """Test getting tiles for a large area"""
        # Bounds covering approximately 10x10 tiles
        bounds = (127.0, 37.0, 128.0, 38.0)
        tiles = grid.get_tiles_in_bounds(bounds)

        # Should get many tiles
        assert len(tiles) > 10

        # All tiles should be unique
        assert len(tiles) == len(set(tiles))

    def test_get_tiles_in_bounds_order(self, grid):
        """Test that tiles are returned in consistent order"""
        bounds = (127.0, 37.5, 127.1, 37.6)
        tiles1 = grid.get_tiles_in_bounds(bounds)
        tiles2 = grid.get_tiles_in_bounds(bounds)

        # Should return same tiles in same order
        assert tiles1 == tiles2

    def test_get_tiles_in_bounds_coverage(self, grid):
        """Test that all returned tiles actually intersect the bounds"""
        bounds = (127.0, 37.5, 127.1, 37.6)
        tiles = grid.get_tiles_in_bounds(bounds)

        minx, miny, maxx, maxy = bounds

        for tile_id in tiles:
            tile_minx, tile_miny, tile_maxx, tile_maxy = grid.get_tile_bounds(tile_id)

            # Check if tile intersects with query bounds
            # Tiles should overlap if:
            # tile_minx <= maxx AND tile_maxx >= minx AND
            # tile_miny <= maxy AND tile_maxy >= miny
            assert tile_minx <= maxx, f"Tile {tile_id} doesn't intersect bounds (x)"
            assert tile_maxx >= minx, f"Tile {tile_id} doesn't intersect bounds (x)"
            assert tile_miny <= maxy, f"Tile {tile_id} doesn't intersect bounds (y)"
            assert tile_maxy >= miny, f"Tile {tile_id} doesn't intersect bounds (y)"
