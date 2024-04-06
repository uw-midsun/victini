import sys
from pathlib import Path

import psycopg2
import questionary
from termcolor import colored
import fsgp_routemodel.fsgp_routemodel as fsgp


def validate_db_creds():
    answers = questionary.form(
        db_host=questionary.text("Database host:port", default=""),
        db_name=questionary.text("Database name", default=""),
        db_user=questionary.text("Database user", default=""),
        db_password=questionary.password("Database user password", default=""),
    ).ask()
    db_user = answers["db_user"]
    db_password = answers["db_password"]
    db_host = answers["db_host"]
    db_name = answers["db_name"]
    host, port = db_host.split(":")
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=db_user, password=db_password, dbname=db_name
        )
        conn.close()
        return True, db_user, db_password, db_host, db_name
    except Exception as e:
        print(colored(e, "red"))
        return False, None, None, None, None


def validate_path(path, extension):
    path = Path(path).absolute()
    return path.is_file() and path.suffix == extension


def cmd_routemodel_import():
    answers = questionary.form(
        track_data_filepath=questionary.path(
            "Path to track data (csv file)",
            default="",
            validate=lambda p: validate_path(p, ".csv"),
        ),
        confirm=questionary.confirm(
            "Confirm import operation",
            default=False,
            auto_enter=False,
        ),
    ).ask()
    track_data_filepath = answers["track_data_filepath"]
    confirm = answers["confirm"]

    if not confirm:
        print(colored("fsgp track data import cancelled", "red"))
    else:
        fsgp.import_track_segments(track_data_filepath)
        print(colored("fsgp track data imported", "green"))


def cmd_routemodel_construct():
    answers = questionary.form(
        route_name=questionary.text("Name of route"),
        segment_data_filepath=questionary.path(
            "Path to segment data (json file)",
            default="",
            validate=lambda p: validate_path(p, ".json"),
        ),
        segment_order=questionary.text("Order of track segments"),
        num_loops=questionary.text("Number of loops"),
        confirm=questionary.confirm(
            "Confirm import operation",
            default=False,
            auto_enter=False,
        ),
    ).ask()
    segment_data_filepath = answers["segment_data_filepath"]
    segment_order = answers["segment_order"]
    num_loops = answers["num_loops"]
    route_name = answers["route_name"]
    confirm = answers["confirm"]

    if not confirm:
        print(colored("fsgp routemodel constuctions cancelled", "red"))
    else:
        fsgp.construct_route(
            segment_data_filepath, segment_order, int(num_loops), route_name
        )
        print(colored("fsgp routemodel constructed", "green"))


def cmd_routemodel_seed(db_user, db_password, db_host, db_name):
    answers = questionary.form(
        route_file_path=questionary.path(
            "Path to route data (json file)",
            default="",
            validate=lambda p: validate_path(p, ".json"),
        ),
        confirm=questionary.confirm(
            "Confirm import operation",
            default=False,
            auto_enter=False,
        ),
    ).ask()
    route_file_path = answers["route_file_path"]
    confirm = answers["confirm"]

    if not confirm:
        print(colored("fsgp database seed cancelled", "red"))
    else:
        print("1) Parsing fsgp routemodel json format to csv format...")
        csv_filepath = fsgp.route_json_to_gdf(route_file_path)
        print("2) Seeding fsgp routemodel csv into database...")
        fsgp.seed_from_csv(csv_filepath, db_user, db_password, db_host, db_name)
        print(colored("fsgp database seed completed", "green"))


def route_model(db_user, db_password, db_host, db_name):
    while True:
        route_model_option = questionary.select(
            "Select FSGP routemodel operation",
            choices=[
                "Import Track Data",
                "Construct Track Routemodel",
                "Seed Database",
                "Exit",
            ],
        ).ask()

        if route_model_option == "Import Track Data":
            cmd_routemodel_import()
        elif route_model_option == "Construct Track Routemodel":
            cmd_routemodel_construct()
        elif route_model_option == "Seed Database":
            cmd_routemodel_seed(db_user, db_password, db_host, db_name)
        else:
            break


if __name__ == "__main__":
    print(
        colored(
            "Using this ETL script requires your database to be updated and working with db_gateway",
            "yellow",
        )
    )

    db_user, db_password, db_host, db_name = None, None, None, None
    auth, db_user, db_password, db_host, db_name = validate_db_creds()
    if auth is False:
        print(colored("Incorrect database credentials", "red"))
        sys.exit(1)

    etl_name = questionary.select(
        "Select FSGP ETL operation",
        choices=[
            "routemodel",
            "location_service",
            "weather",
            "speed_limit/street_names",
            "drop_tables",
        ],
    ).ask()

    if etl_name == "routemodel":
        route_model(db_user, db_password, db_host, db_name)
    elif etl_name == "location_service":
        print(colored("NOT IMPLEMENTED", "red"))
    elif etl_name == "weather":
        print(colored("NOT IMPLEMENTED", "red"))
    elif etl_name == "speed_limit/street_names":
        print(colored("NOT IMPLEMENTED", "red"))
    elif etl_name == "drop_tables":
        print(colored("NOT IMPLEMENTED", "red"))
