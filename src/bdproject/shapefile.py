import geopandas as gpd

from bdproject.paths import get_path_shapefile


def get_gdf_shapefile() -> gpd.GeoDataFrame:
    return gpd.read_file(get_path_shapefile())
