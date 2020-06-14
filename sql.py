CREATE_TABLE_EVENT_LOGS = """
    CREATE TABLE IF NOT EXISTS public.raw_events (
        raw_event_id        SERIAL PRIMARY KEY,
        created             TIMESTAMP(6) DEFAULT NOW() NOT NULL,
        modified            TIMESTAMP(6) DEFAULT NOW() NOT NULL,
        source_id           SMALLINT NOT NULL CHECK (source_id > 0 AND source_id < 20),
        source_event_id     BIGINT NOT NULL,
        event_logs          TEXT DEFAULT '{}'::jsonb NOT NULL,
        deleted             BOOLEAN DEFAULT FALSE NOT NULL
    );
"""

CREATE_INDEX_SOURCE_EVENT_ID = "CREATE INDEX IF NOT EXISTS idx_source_event_id ON public.raw_events (source_event_id);"

CREATE_INDEX_SOURCE_AND_EVENT_IDS = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_source_id_source_event_id 
        ON public.raw_events (source_id, source_event_id);
"""

INSERT_N_DUMMY_ROWS = """
    INSERT INTO public.raw_events (
        source_id, 
        source_event_id, 
        event_logs
    )
    SELECT
        mod(row_ids.i, 5) + 1,
        row_ids.i,
        \'{event_logs_str}\' as event_logs_str
    FROM generate_series(1, {num_rows}) row_ids(i) 
    ON CONFLICT (source_id, source_event_id)
    DO NOTHING;
"""

UPDATE_SOURCE_LOGS = """
    UPDATE  public.raw_events
    SET     modified = (
                SELECT NOW() + (random() * (INTERVAL '90 days'))
            ), 
            event_logs = \'{event_logs_updated}\'
    WHERE source_id = {source_id};
"""

SOFT_DELETE_QUERY = """
    UPDATE public.raw_events
    SET    deleted = TRUE, modified = NOW() 
    WHERE  modified >= (
        SELECT NOW() + INTERVAL '60 days'
    )
    AND source_id = {source_id}
    AND deleted IS FALSE;
"""

TABLE_SIZE_QUERY = "SELECT pg_size_pretty(pg_total_relation_size('public.raw_events'));"

TABLE_STATS_QUERY = "SELECT * FROM pg_stat_user_tables WHERE relname = 'raw_events';"

TRUNCATE_TABLE_QUERY = "TRUNCATE TABLE public.raw_events RESTART IDENTITY;"
