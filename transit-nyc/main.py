import pandas
import logging
import click

routes_df = None
stops_df = None
stop_times_df = None
trips_df = None


def find_stop_ids_with_like_station_name(station_name):
    """ Searches stops_df for stop names that contain station name

    :param station_name: (string) target station name
    :return: (list) stop_ids
    """
    global stops_df
    df = stops_df.loc[stops_df['stop_name'].str.contains(station_name)]
    return list(df['stop_id'])


def find_trip_ids_with_stop_ids(stop_ids):
    """ Searches stop_times_df for stops that are in stop_ids. Returns the trip_id

    :param stop_ids: (list) List of possible stop_ids
    :return: (list) trip_ids
    """
    global stop_times_df
    df = stop_times_df.loc[stop_times_df['stop_id'].isin(stop_ids)]
    return list(df['trip_id'])


def find_route_ids_with_trip_ids(trip_ids):
    """ Searches trips_df for trips that match the possible trip_ids. Returns the route_ids

    :param trip_ids: (list) List of possible trip_ids
    :return: (list) route_ids
    """
    global trips_df
    df = trips_df.loc[trips_df['trip_id'].isin(trip_ids)]
    return list(df['route_id'])


def find_passing_station(station_name):
    """ Searches the dataframes to find what routes pass the station_name

    :param station_name: (string) name of a station
    :return: (list) resulting routes
    """
    stop_ids = find_stop_ids_with_like_station_name(station_name)
    trip_ids = find_trip_ids_with_stop_ids(stop_ids)
    route_ids = find_route_ids_with_trip_ids(trip_ids)
    route_ids = list(dict.fromkeys(route_ids))  # remove duplicates
    route_ids.sort()
    return route_ids


def create_df(gtfs_folder):
    """ Creates a sqlite3 db

    :param gtfs_folder: (string) folder of the GTFS data
    :return:
    """
    # Only looking at the files routes, stop_times, stops, and trips
    global routes_df
    routes_df = pandas.read_csv(gtfs_folder + "/routes.txt")

    global stop_times_df
    stop_times_df = pandas.read_csv(gtfs_folder + "/stop_times.txt")

    global stops_df
    stops_df = pandas.read_csv(gtfs_folder + "/stops.txt")

    global trips_df
    trips_df = pandas.read_csv(gtfs_folder + "/trips.txt")



@click.command()
@click.option('--db_folder', default="./GTFS", help="Location of the GTFS data folder")
@click.option('--station_name', prompt='station name', help='The target station')
@click.option('--debug', default='False', help='turn on debug')
def print_pass_station(db_folder, station_name, debug):
    if debug.lower() == "true":
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    create_df(db_folder)

    route_ids = find_passing_station(station_name)
    print(route_ids)


if __name__ == "__main__":
    print_pass_station()
