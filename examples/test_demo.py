"""
Test script for demo_timeseries.py

Creates mock COG files and runs the demo to verify it works.
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds


def create_mock_cog_files(output_dir: Path, num_files: int = 3):
    """
    Create mock COG files for testing

    Args:
        output_dir: Directory to save mock files
        num_files: Number of time steps to create
    """
    print(f"Creating {num_files} mock COG files...")

    bounds = (126.95, 37.50, 126.96, 37.51)
    width, height = 100, 100
    transform = from_bounds(*bounds, width, height)

    for month in range(1, num_files + 1):
        filename = f"S2A_2024{month:02d}15_T52SDG.tif"
        filepath = output_dir / filename

        # Create realistic 4-band data (blue, green, red, nir)
        # Simulate vegetation with varying NDVI across time
        data = np.zeros((4, height, width), dtype=np.uint16)

        # Seasonal variation: higher NDVI in spring/summer
        ndvi_factor = 0.5 + 0.3 * np.sin(month * np.pi / 6)

        # Blue
        data[0, :, :] = np.random.randint(500, 1500, (height, width))
        # Green
        data[1, :, :] = np.random.randint(800, 2000, (height, width))
        # Red (lower for vegetation)
        data[2, :, :] = np.random.randint(300, 1000, (height, width))
        # NIR (higher for vegetation, varies seasonally)
        base_nir = 2000 + int(ndvi_factor * 2000)
        data[3, :, :] = np.random.randint(base_nir, base_nir + 2000, (height, width))

        # Write COG with metadata
        with rasterio.open(
            filepath,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=4,
            dtype=np.uint16,
            crs="EPSG:4326",
            transform=transform,
            nodata=0,
        ) as dst:
            dst.write(data)
            # Add acquisition time to metadata
            dst.update_tags(TIFFTAG_DATETIME=f"2024:{month:02d}:15 10:30:00")

        print(f"  ✓ Created {filename}")


def main():
    """Run demo test"""
    print("=" * 60)
    print("Testing demo_timeseries.py")
    print("=" * 60)

    # Create temporary directories
    temp_dir = Path(tempfile.mkdtemp())
    cog_dir = temp_dir / "cogs"
    warehouse_dir = temp_dir / "warehouse"

    cog_dir.mkdir(exist_ok=True)
    warehouse_dir.mkdir(exist_ok=True)

    try:
        # Create mock COG files
        create_mock_cog_files(cog_dir, num_files=3)

        print("\n" + "=" * 60)
        print("Running demo_timeseries.py...")
        print("=" * 60 + "\n")

        # Run demo script
        result = subprocess.run(
            [
                sys.executable,
                "examples/demo_timeseries.py",
                "--cog-dir",
                str(cog_dir),
                "--warehouse",
                str(warehouse_dir),
            ],
            capture_output=True,
            text=True,
        )

        # Print output
        print(result.stdout)

        if result.returncode != 0:
            print("\n❌ Demo failed!")
            print("STDERR:")
            print(result.stderr)
            sys.exit(1)
        else:
            print("\n✓ Demo test passed!")

        # Verify warehouse was created
        if not warehouse_dir.exists():
            print("❌ Warehouse directory not created!")
            sys.exit(1)

        # Check for tiles directory
        tiles_dir = warehouse_dir / "tiles"
        if not tiles_dir.exists():
            print("❌ Tiles directory not created!")
            sys.exit(1)

        print(f"\n✓ Warehouse structure verified at {warehouse_dir}")

    finally:
        # Cleanup
        print(f"\nCleaning up temporary files at {temp_dir}...")
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("✓ Cleanup complete")


if __name__ == "__main__":
    main()
