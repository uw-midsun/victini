import pandas as pd
import numpy as np
import json
from pathlib import Path
from sqlalchemy import create_engine, inspect, String
import geopandas as gpd
from geoalchemy2 import Geometry
from geopy import distance


def import_track_segments(track_data_filepath):
    file = Path(track_data_filepath)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the location specified")

    segments = {}
    dtypes = {"WKT": str, "name": str, "description": str}
    df = pd.read_csv(file, dtype=dtypes)
    df.dropna(subset=["WKT"], inplace=True)
    for _, row in df.iterrows():
        linestring, name = row["WKT"], row["name"]
        linestring = linestring[12:-1]
        points = []
        for p in linestring.split(","):
            p = p.strip().split(" ")
            lng, lat = p[0], p[1]
            points.append({"lng": float(lng), "lat": float(lat)})
        segments[name] = points

    file_path = file.parent / f"{file.stem}_segments.json"
    with open(file_path, "w") as f:
        json.dump(segments, f)
    return file_path


def construct_route(segments_data_filepath, segment_order, num_loops, route_name):
    file = Path(segments_data_filepath)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the location specified")

    segments = {}
    with open(file, "r") as f:
        segments = json.load(f)

    segment_order = segment_order.split(" ")
    route_points = [segments[o] for o in segment_order]
    route_points = [point for rps in route_points * num_loops for point in rps]
    route = {"points": route_points}

    file_path = file.parent / f"{route_name}.json"
    with open(file_path, "w") as f:
        json.dump(route, f)
    return file_path


def route_json_to_gdf(route_file_path):
    file = Path(route_file_path)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the location specified")
    route_json = {}
    gdf_rows = []
    with open(file, "r") as f:
        route_json = json.load(f)
    for point in route_json["points"]:
        lon = point.get("lng", 0)
        lat = point.get("lat", 0)
        data = {
            "lon": lon,
            "lat": lat,
            "geo": "POINT({} {})".format(lon, lat),
            "geopy_elapsed_dist_m": 0,
            "geopy_dist_from_last_m": 0,
        }
        if len(gdf_rows) > 0:
            prev = gdf_rows[-1]
            prev_coor = (prev["lat"], prev["lon"])
            curr_coor = (data["lat"], data["lon"])
            dist = distance.great_circle(prev_coor, curr_coor).m
            data["geopy_dist_from_last_m"] = dist
            data["geopy_elapsed_dist_m"] = prev["geopy_elapsed_dist_m"] + dist
        gdf_rows.append(data)

    gdf = gpd.GeoDataFrame(gdf_rows)
    csv_filepath = file.with_suffix(".csv")
    gdf.to_csv(csv_filepath)
    return csv_filepath


def seed_from_csv(track_csv_filepath, db_user, db_password, db_host, db_name):
    file = Path(track_csv_filepath)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the location specified")
    gdf = gpd.GeoDataFrame(pd.read_csv(file))
    gdf.fillna(np.nan).replace([np.nan], [None])
    gdf = gdf.drop(columns=["Unnamed: 0"])
    gdf.index.name = "id"

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
    )

    gdf.index += 1  # Making table 1-indexed
    if inspect(engine).has_table("routemodelfsgp"):
        row_len = pd.read_sql_query(
            sql="SELECT COUNT(*) FROM routemodelfsgp", con=engine
        )
        gdf.index += row_len.iloc[0, 0]

    response = gdf.to_sql(
        name="routemodelfsgp",
        con=engine,
        schema="public",
        if_exists="append",
        index=True,
        method="multi",
        dtype={"geo": Geometry("POINT", srid=4326), "street_name": String},
    )

    if gdf.shape[0] != response:
        raise SystemError("dataframe insertion failed")
    else:
        print("routemodel dataframe insertion success")
