import configparser
import time
import timeit

from functools import wraps

import psycopg2
import structlog

from faker import Faker
from sql import (CREATE_TABLE_EVENT_LOGS,
                 CREATE_INDEX_SOURCE_EVENT_ID,
                 CREATE_INDEX_SOURCE_AND_EVENT_IDS,
                 INSERT_N_DUMMY_ROWS,
                 SOFT_DELETE_QUERY,
                 TABLE_SIZE_QUERY,
                 TABLE_STATS_QUERY,
                 TRUNCATE_TABLE_QUERY,
                 UPDATE_SOURCE_LOGS)


# Setup logging
logger = structlog.get_logger()

# Read config
config = configparser.ConfigParser()
config.read('demo_config.cfg')

DATABASE_URL = config['POSTGRES']['DATABASE_URL']
VERBOSE = bool(config['COMMON']['VERBOSE'])

Faker.seed(0)
faker = Faker()
_sleep_duration_seconds = 120


def measure_time(func):
    @wraps(func)
    def timed_func(*args, **kwargs):
        start_time = timeit.default_timer()
        func()
        elapsed = timeit.default_timer() - start_time
        logger.info('Function "{name}" took {time} seconds to complete.'.format(name=func.__name__, time=elapsed))
    return timed_func


def connect_to_events_db():
    return psycopg2.connect(DATABASE_URL)


def create_events_table():
    with connect_to_events_db() as db:
        with db.cursor() as cur:
            cur.execute(CREATE_TABLE_EVENT_LOGS)
            cur.execute(CREATE_INDEX_SOURCE_EVENT_ID)
            cur.execute(CREATE_INDEX_SOURCE_AND_EVENT_IDS)
        db.commit()
    logger.info("Table created")


@measure_time
def generate_dummy_data():
    logger.info("Starting dummy data insertion")
    num_rows_to_generate = int(config['COMMON']['NUM_ROWS_TO_GENERATE'])
    event_log_text = faker.text(max_nb_chars=2000)

    insert_query = INSERT_N_DUMMY_ROWS.format(event_logs_str=event_log_text, num_rows=num_rows_to_generate)
    if VERBOSE:
        logger.info(f"{insert_query}")

    with connect_to_events_db() as db:
        with db.cursor() as cur:
            logger.info("Starting insertion...")
            cur.execute(insert_query)
        db.commit()

        logger.info(f"{cur.rowcount} rows generated in events table")
        _print_table_stats(db)


@measure_time
def update_data():
    logger.info("Starting to update data")
    event_log_updated_text = faker.text(max_nb_chars=1000)

    with connect_to_events_db() as db:
        all_source_ids = _get_all_source_ids()

        for source_id in all_source_ids:
            source_id = source_id[0]
            with db.cursor() as cur:
                update_query = UPDATE_SOURCE_LOGS.format(
                    event_logs_updated=event_log_updated_text,
                    source_id=source_id
                )
                cur.execute(update_query)
                logger.info(f"Update {cur.rowcount} rows for source_id: {source_id}")
            db.commit()

        logger.info("All rows in events table are updated")
        _print_table_stats(db)


@measure_time
def soft_delete_rows():
    logger.info("Starting soft-deletion")
    rows_deleted = 0
    with connect_to_events_db() as db:
        all_source_ids = _get_all_source_ids()

        for source_id in all_source_ids:
            source_id = source_id[0]
            with db.cursor() as cur:
                cur.execute(
                    SOFT_DELETE_QUERY.format(source_id=source_id)
                )
                rows_deleted = rows_deleted + cur.rowcount
                logger.info(f"{cur.rowcount} rows soft deleted for source_id: {source_id}")
            db.commit()

        logger.info(f"Total soft deleted rows: {rows_deleted}")
        _print_table_stats(db)


def truncate_table():
    with connect_to_events_db() as db:
        with db.cursor() as cur:
            cur.execute(TRUNCATE_TABLE_QUERY)
        db.commit()

        logger.info("Truncated events table")


def _get_all_source_ids():
    with connect_to_events_db() as db:
        with db.cursor() as cur:
            cur.execute("SELECT DISTINCT source_id FROM public.raw_events")
            all_source_ids = cur.fetchall()

    return all_source_ids


def _print_table_stats(db_conn):
    with db_conn:
        with db_conn.cursor() as cur:
            cur.execute(TABLE_SIZE_QUERY)
            res = cur.fetchone()
            logger.info(f"raw_events table size: {res}")

            cur.execute(TABLE_STATS_QUERY)
            res_stats = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]

            logger.info("raw_events table stats:")
            for col_name, col_stat in zip(col_names, res_stats):
                logger.info(f"{col_name:25}: {col_stat}")


def main():
    create_events_table()
    # truncate_table()

    generate_dummy_data()

    update_data()
    soft_delete_rows()
    logger.msg("All done!")


if __name__ == '__main__':
    for i in range(1, 6):
        logger.info(f"RUN No :::::: {i}")
        main()
        logger.info(f"Sleeping for {_sleep_duration_seconds} seconds after RUN No: {i}")
        time.sleep(_sleep_duration_seconds)
