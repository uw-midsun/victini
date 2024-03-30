import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from geoalchemy2 import Geometry
from geopy import distance
from sqlalchemy import create_engine, inspect, String


def gpx_json_to_gdf(json_filepath):
    file = Path(json_filepath)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the specified location")

    gdf_rows = []
    with open(json_filepath, "r") as f:
        gpx = json.load(f)
    for point in gpx["points"]:
        lon = point.get("lng", 0)
        lat = point.get("lat", None)
        data = {
            "lon": lon,
            "lat": lat,
            "geo": "POINT({} {})".format(lon, lat),
            "type": point.get("type", None),
            "street_name": None,
            "step": point.get("step", None),
            "next_turn": point.get("nextturn", None),
            "dir": None,
            "speed_limit_km_per_h": None,
            "gpx_dist_to_next_waypoint_m": None,
            "gpx_elapsed_dist_m": None,
            "geopy_elapsed_dist_m": 0,
            "geopy_dist_from_last_m": 0,
            "weather_id": None,
        }
        if dir := point.get("dir", None):
            soup = BeautifulSoup(dir, "html.parser")
            data["dir"] = soup.get_text()
        if dist := point.get("dist", None):
            data["gpx_dist_to_next_waypoint_m"] = dist["val"]
            data["gpx_elapsed_dist_m"] = dist["total"] - dist["val"]
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


def seed_from_csv(
    csv_filepath, table, override_option, db_user, db_password, db_host, db_name
):
    file = Path(csv_filepath)
    if not file.is_file():
        raise FileNotFoundError("No file exists at the specified location")
    gdf = gpd.GeoDataFrame(pd.read_csv(file))
    gdf.fillna(np.nan).replace([np.nan], [None])
    gdf = gdf.drop(columns=["Unnamed: 0"])
    gdf.index.name = "id"

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
    )

    if override_option == "override" and inspect(engine).has_table(table):
        with engine.connect() as conn:
            conn.execute(f"TRUNCATE TABLE {table}")

    if override_option == "append" and inspect(engine).has_table(table):
        row_len = pd.read_sql_query(sql=f"SELECT COUNT(*) FROM {table}", con=engine)
        gdf.index += row_len.iloc[0, 0] + 1

    response = gdf.to_sql(
        name=table,
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
        print(f"{table} dataframe insertion success")


def main(
    gpx_json_filepath, table, override_option, db_user, db_password, db_host, db_name
):
    print("1) Parsing gpx json format to csv format...")
    csv_filepath = gpx_json_to_gdf(gpx_json_filepath)

    print("2) Seeding data into the database...")
    seed_from_csv(
        csv_filepath, table, override_option, db_user, db_password, db_host, db_name
    )


if __name__ == "__main__":
    gpx_json_filepath = ""
    table = ""
    override_option = "append"  # Or "override"
    db_user = ""
    db_password = ""
    db_host = ""
    db_name = ""
    main(
        gpx_json_filepath,
        table,
        override_option,
        db_user,
        db_password,
        db_host,
        db_name,
    )
