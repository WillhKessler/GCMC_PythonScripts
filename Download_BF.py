"""
This snippet demonstrates how to access and convert the buildings
data from .csv.gz to geojson for use in common GIS tools. You will
need to install pandas, geopandas, and shapely.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import fiona

"""
This snippet demonstrates how to access and convert the buildings
data from .csv.gz to geojson for use in common GIS tools. You will
need to install pandas, geopandas, and shapely.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape


def main():
    # this is the name of the geography you want to retrieve. update to meet your needs
    location = "UnitedStates"

    dataset_links = pd.read_csv(
        "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv"
    )
    greece_links = dataset_links[dataset_links.Location == location]
    geodf = gpd.GeoDataFrame(columns=["id", "geometry"], geometry="geometry", crs=4326)

    # row = greece_links.iloc[0,]
    print("starting processing. ")
    for _, row in greece_links.iterrows():
        df = pd.read_json(row.Url, lines=True)
        df["geometry"] = df["geometry"].apply(shape)
        gdf = gpd.GeoDataFrame(df, crs=4326)
        gdf.to_file(
            r"S:\GCMC\Data\BuiltEnvironment\BuildingFootprints\MS_BF_heights_"
            + str(_)
            + ".shp",
            driver="ESRI Shapefile",
        )
        # geodf = pd.concat([geodf, gdf])
        # gdf.to_file(f"{row.QuadKey}.geojson", driver="GeoJSON")
        print(
            "Finished Row",
            _,
            ". Starting nextrow: ",
            _ + 1,
            "out of ",
            max(greece_links.index),
        )
    print("finished reading json... writing.")

    print("done")


if __name__ == "__main__":
    main()
