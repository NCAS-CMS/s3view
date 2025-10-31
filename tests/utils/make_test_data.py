import cfdm
import numpy as np
from datetime import datetime
from pathlib import Path

def make_test_netcdf_with_coords(tmp_path: Path = Path("/tmp")) -> Path:
    """Create a CF-compliant NetCDF file where each lat/lon slice ≈ 1 MB."""
    ncfile = tmp_path / "test_dummy_narnia_ignore-this-stuff.nc"

    n_time = 12
    n_lat = 362
    n_lon = 362

    # ---------------------------------------------------------------------
    # Create Field 1: temperature
    # ---------------------------------------------------------------------
    f1 = cfdm.Field()
    f1.set_properties({
        "standard_name": "air_temperature",
        "units": "K",
        "experiment": "dummy",
        "myattribute": "value",
        "cell_methods": "time: mean",
    })
    f1.nc_set_variable("temp")

    # Define axes
    time = f1.set_construct(cfdm.DomainAxis(size=n_time))
    f1.set_construct(cfdm.DomainAxis(size=n_lat), key="lat")
    f1.set_construct(cfdm.DomainAxis(size=n_lon), key="lon")

    # Time coordinate
    times = cfdm.DimensionCoordinate()
    times.set_properties({
        "standard_name": "time",
        "units": "days since 2000-01-01",
        "calendar": "gregorian",
    })
    times.set_data(cfdm.Data(np.arange(15, 365, 30.4)[:n_time]))
    times.nc_set_variable("time")
    f1.set_construct(times, axes=time)

    # Latitude coordinate
    lats = cfdm.DimensionCoordinate()
    lats.set_properties({
        "standard_name": "latitude",
        "units": "degrees_north",
    })
    lats.set_data(cfdm.Data(np.linspace(-90, 90, n_lat)))
    lats.nc_set_variable("lat")
    f1.set_construct(lats, axes=["lat"])

    # Longitude coordinate
    lons = cfdm.DimensionCoordinate()
    lons.set_properties({
        "standard_name": "longitude",
        "units": "degrees_east",
    })
    lons.set_data(cfdm.Data(np.linspace(0, 360, n_lon, endpoint=False)))
    lons.nc_set_variable("lon")
    f1.set_construct(lons, axes=["lon"])

    # Field data (12 × 362 × 362)
    data1 = 280 + 10 * np.random.rand(n_time, n_lat, n_lon)
    f1.set_data(cfdm.Data(data1, units="K"), axes=[time, "lat", "lon"])

    # ---------------------------------------------------------------------
    # Create Field 2: pressure (reuses same coordinate constructs)
    # ---------------------------------------------------------------------
    f2 = f1.copy()
    f2.set_property("standard_name", "air_pressure")
    f2.set_property("units", "Pa")
    f2.nc_set_variable("press")
    f2.set_data(cfdm.Data(100000 + 500 * np.random.rand(n_time, n_lat, n_lon), units="Pa"))

    # ---------------------------------------------------------------------
    # Write to file
    # ---------------------------------------------------------------------
    cfdm.write([f1, f2], ncfile)
    print(f"Wrote {ncfile} ({ncfile.stat().st_size/1e6:.2f} MB)")
    return ncfile