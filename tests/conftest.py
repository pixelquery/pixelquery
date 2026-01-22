"""
PixelQuery Test Configuration

Shared pytest fixtures for all tests.
"""

import pytest


# Example fixtures (to be implemented)
@pytest.fixture
def sample_warehouse(tmp_path):
    """Create a temporary warehouse for testing"""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()
    return str(warehouse_path)


@pytest.fixture
def sample_tile_id():
    """Standard test tile ID"""
    return "x0024_y0041"
