# data_generator.py
import time
import os
# import configparser
from argparse import ArgumentParser

import pandas as pd
import psycopg2
from sklearn.datasets import load_breast_cancer

# config = configparser.ConfigParser()
# config.read('../config.ini')
# PSQLUSER = config['DEFAULT']['PSQLUSER']
# PSQLPWD = config['DEFAULT']['PSQLPWD']
# PSQLPORT = int(config['DEFAULT']['PSQLPORT'])
os.environ['PSQL_USER'] = "nkeum"
os.environ['PSQL_PWD'] = "qwer123!"
os.environ['PSQL_PORT'] = "5432"


def get_data():
    X, y = load_breast_cancer(return_X_y=True, as_frame=True)
    df = pd.concat([X, y], axis="columns")

    # column rename(get rid of space)
    cols = list(df.columns)
    for i in range(len(cols)):
        cols[i] = cols[i].replace(" ", "_")

    rename_rule = {}

    for i in range(len(cols)):
        rename_rule[list(df.columns)[i]] = cols[i]
    df = df.rename(columns=rename_rule)
    return df


def create_table(db_connect, data):
    create_table_query = """CREATE TABLE IF NOT EXISTS cancer_data (
        id SERIAL PRIMARY KEY,
        timestamp timestamp,
    """

    for col in list(data.columns):
        if col == "target":
            create_table_query += "\t"+col+" int\n"
        else:
            create_table_query += "\t"+col+" float8,\n"
    create_table_query += ");"

    print(create_table_query)
    with db_connect.cursor() as cur:
        cur.execute(create_table_query)
        db_connect.commit()


def insert_data(db_connect, data):
    insert_row_query = f"""
    INSERT INTO cancer_data
    (timestamp, mean_radius, mean_texture, mean_perimeter, mean_area, mean_smoothness, mean_compactness, mean_concavity, mean_concave_points, mean_symmetry, mean_fractal_dimension, radius_error, texture_error, perimeter_error, area_error, smoothness_error, compactness_error, concavity_error, concave_points_error, symmetry_error, fractal_dimension_error, worst_radius, worst_texture, worst_perimeter, worst_area, worst_smoothness, worst_compactness, worst_concavity, worst_concave_points, worst_symmetry, worst_fractal_dimension, target)
        VALUES (
            NOW(),
            {data.mean_radius},
            {data.mean_texture},
            {data.mean_perimeter},
            {data.mean_area},
            {data.mean_smoothness},
            {data.mean_compactness},
            {data.mean_concavity},
            {data.mean_concave_points},
            {data.mean_symmetry},
            {data.mean_fractal_dimension},
            {data.radius_error},
            {data.texture_error},
            {data.perimeter_error},
            {data.area_error},
            {data.smoothness_error},
            {data.compactness_error},
            {data.concavity_error},
            {data.concave_points_error},
            {data.symmetry_error},
            {data.fractal_dimension_error},
            {data.worst_radius},
            {data.worst_texture},
            {data.worst_perimeter},
            {data.worst_area},
            {data.worst_smoothness},
            {data.worst_compactness},
            {data.worst_concavity},
            {data.worst_concave_points},
            {data.worst_symmetry},
            {data.worst_fractal_dimension},
            {data.target}
        );
    """
    print(insert_row_query)
    with db_connect.cursor() as cur:
        cur.execute(insert_row_query)
        db_connect.commit()


def generate_data(db_connect, df):
    while True:
        insert_data(db_connect, df.sample(1).squeeze())
        time.sleep(1)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--db-host", dest="db_host",
                        type=str, default="localhost")
    args = parser.parse_args()

    db_connect = psycopg2.connect(
        user=os.environ['PSQL_USER'],
        password=os.environ['PSQL_PWD'],
        host=args.db_host,
        port=os.environ['PSQL_PORT'],
        database="mydatabase",
    )

    df = get_data()
    create_table(db_connect, df)
    generate_data(db_connect, df)
