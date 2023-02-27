# create_table.py
import psycopg2
import pandas as pd
from sklearn.datasets import load_breast_cancer


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


if __name__ == "__main__":
    db_connect = psycopg2.connect(
        user="targetuser",
        password="targetpassword",
        host="target-postgres-server",
        port=5432,
        database="targetdatabase",
    )
    df = get_data()
    create_table(db_connect, df)
