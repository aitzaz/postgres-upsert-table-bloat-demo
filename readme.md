# Postgres Table Bloating Demo

## Overview
Demo code for my blog post titled [When Indexes & Performance Tuning don’t fix Slow queries in Postgres](https://medium.com/@aitzaz/when-indexes-performance-tuning-dont-fix-slow-queries-in-postgres-fb0a63d99276).

## Setup & Run Guide

### Pre-requisites
Make sure you have following setup:

- Postgres 9.6 installed.
- Update `DATABASE_URL` in `demo_config.cfg`; OR
    - A user named `blog_demo` with password `blog_demo_pwd`. 
    - A database named `events_dwh`.
- `Pyenv` and `Pipenv` are installed.
- Around 20GBs of disk space on hard drive (depends on how many runs you want to try). 

### Run script

```shell script
# activate env
pipenv shell

# install dependencies
pipenv install

# run script
python table_bloat_demo.py
```

By default, script will generate 2.5 million rows but you can configure it from `demo_config.cfg`.

## Monitor database size and Table bloat fix

### Monitoring
Script will generate output logs on console printing table size after each update and also other table related stats. You can also do that manually by running these queries after connecting to DB:

- Table size

`SELECT pg_size_pretty(pg_total_relation_size('raw_events'));`

- Table stats

`SELECT * FROM pg_stat_user_tables WHERE relname = 'raw_events';` 

### Vacuum/Analyse to fix table bloat

`VACUUM ANALYSE public.raw_events;`

You can use `VACUUM FULL` as well but beware that it will LOCK the table.
