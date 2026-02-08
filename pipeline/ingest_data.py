#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click
from tqdm import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', required=True)
@click.option('--pg-pass', required=True)
@click.option('--pg-host', required=True)
@click.option('--pg-port', default=5432, type=int)
@click.option('--pg-db', required=True)
@click.option('--table-name', required=True)
@click.option('--url', required=True)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, table_name, url):

    engine = create_engine(
        f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100_000
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists='replace'
            )
            first = False
            print("✅ Table created")

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='append'
        )

        print("Inserted:", len(df_chunk))


if __name__ == "__main__":
    main()
