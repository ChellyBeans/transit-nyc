import pandas
import sqlite3
import logging
import click

gtfs_db = None  # This will be our db


def create_table(csv, table_name):
    """ Grabs the csv and inserts data into sqlite3 db

    :param csv: (string) path to csv
    :param table_name: (string) name for table
    :return:
    """
    if not gtfs_db:
        logging.error("database not initialized")
        return

    logging.info("importing %s..." % table_name)
    p = pandas.read_csv(csv)
    p.to_sql(table_name, gtfs_db)
    logging.info("%s imported." % table_name)


def convert_single_tuple_list_to_list(single_tuple_list):
    """ Helper function

    :param single_tuple_list: (list) list of tuple items that contain one value
    :return: (list)
    """
    new_list = []
    for tup in single_tuple_list:
        new_list.append(tup[0])
    return new_list


def find_stop_id_station_name_like(station_name):
    """ Searches the stops table for stops with containing station name

    :param station_name: (string) station name
    :return: (list) stop_ids
    """
    if not gtfs_db:
        logging.error("database not initialized")
        return []

    cursor = gtfs_db.cursor()
    query = 'SELECT DISTINCT stop_id FROM stops WHERE stop_name LIKE \'%' + station_name + '%\''
    cursor.execute(query)
    stops = cursor.fetchall()
    result = convert_single_tuple_list_to_list(stops)
    logging.debug(query + ": found " + str(result.__len__()) + " stops. " + str(result))
    return result


def find_trip_id_with_stop_id(stop_ids):
    """ Searches stop times for stop_ids matches any stop_ids in list. Returns results trip_ids

    :param stop_ids: (list) list of possible stop_ids
    :return: (list) trip_ids
    """
    if not gtfs_db:
        logging.error("database not initialized")
        return []

    if not stop_ids:
        return []

    cursor = gtfs_db.cursor()
    query = 'SELECT DISTINCT trip_id FROM stop_times WHERE stop_id = \'' + '\' OR stop_id = \''.join(stop_ids) + '\''

    cursor.execute(query)
    trips = cursor.fetchall()
    result = convert_single_tuple_list_to_list(trips)
    logging.debug(query + ": found " + str(result.__len__()) + " trips. " + str(result))
    return result


def find_route_with_trip_id(trip_ids):
    """ Searches trips for trips with any of the trip_ids. Returns the resulting route_ids

    :param trip_ids: (list) possible trip_ids
    :return: (list) route_ids
    """
    if not gtfs_db:
        logging.error("database not initialized")
        return []

    if not trip_ids:
        return []

    cursor = gtfs_db.cursor()
    query = 'SELECT DISTINCT route_id FROM trips WHERE trip_id = \'' + '\' OR trip_id = \''.join(trip_ids) + '\''
    cursor.execute(query)
    routes = cursor.fetchall()
    result = convert_single_tuple_list_to_list(routes)
    logging.debug(query + ": found " + str(result.__len__()) + " routes. " + str(result))
    return result


def find_passing_station(station_name):
    """ Searches the database to find what routes pass the station_name

    :param station_name: (string) name of a station
    :return: (list) resulting routes
    """
    if not gtfs_db:
        logging.error("database not initialized")
        return []

    stop_ids = find_stop_id_station_name_like(station_name)  # find stops
    trip_ids = find_trip_id_with_stop_id(stop_ids)  # find trips that day at stops

    # We kinda get a lot of trip_ids, too many for a query...
    trip_ids_chunks = []
    for i in range(0, len(trip_ids), 100):  # No reason why I chose 100
        trip_ids_chunks.append(trip_ids[i:i + 100])

    route_ids = []  # find routes that made those trips
    for trip_ids in trip_ids_chunks:
        r_ids = find_route_with_trip_id(trip_ids)
        for route in r_ids:
            if route not in route_ids:
                route_ids.append(route)
    route_ids.sort()
    return route_ids


def create_db(gtfs_folder):
    """ Creates a sqlite3 db

    :param gtfs_folder: (string) folder of the GTFS data
    :return:
    """
    global gtfs_db
    gtfs_db = sqlite3.connect(":memory:")  # In RAM
    # Only looking at the files routes, stop_times, stops, and trips
    create_table(gtfs_folder + "/routes.txt", "routes")
    create_table(gtfs_folder + "/stop_times.txt", "stop_times")
    create_table(gtfs_folder + "/stops.txt", "stops")
    create_table(gtfs_folder + "/trips.txt", "trips")


def close_db():
    """ Close the SQLite DB if possible

    :return:
    """
    if gtfs_db:
        gtfs_db.close()


@click.command()
@click.option('--db_folder', default="./GTFS", help="Location of the GTFS data folder")
@click.option('--station_name', prompt='station name', help='The target station')
@click.option('--debug', default='False', help='turn on debug')
def print_pass_station(db_folder, station_name, debug):
    if debug.lower() == "true":
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    create_db(db_folder)

    route_ids = find_passing_station(station_name)
    print(route_ids)

    close_db()


if __name__ == "__main__":
    print_pass_station()
