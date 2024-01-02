import psycopg2
import pandas as pd
import time

start_time = time.time()


def read_csv():
    data = pd.read_csv("data.csv")
    data.dropna(inplace=True)
    data.rename(columns={'index': 'Indexx'}, inplace=True)

    return data


def db_connect():
    db_params = {
        'host': 'localhost',
        'database': 'bd',
        'user': 'postgres',
        'password': 'byebye'
    }
    conn = psycopg2.connect(**db_params)
    return conn


def create_publisher_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Publisher CASCADE;")
        cur.execute("""CREATE TABLE Publisher (
                            Publisher_ID SERIAL PRIMARY KEY,
                            Publisher VARCHAR(255)
                        );""")


def create_developer_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Developer CASCADE;")
        cur.execute("""CREATE TABLE Developer (
                            Developer_ID SERIAL PRIMARY KEY,
                            Developer VARCHAR(255)
                        );""")


def create_game_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Video_Game CASCADE;")
        cur.execute("""CREATE TABLE Video_Game (
                            Indexx INT PRIMARY KEY,
                            Name VARCHAR(255),
                            Platform VARCHAR(50),
                            Year_of_Release INT,
                            Genre VARCHAR(50),
                            Publisher_ID INT,
                            Developer_ID INT,
                            Critic_Score DECIMAL(5, 2),
                            Critic_Count INT,
                            User_Score DECIMAL(5, 2),
                            User_Count INT,
                            Rating VARCHAR(5),
                            FOREIGN KEY (Publisher_ID) REFERENCES Publisher(Publisher_ID),
                            FOREIGN KEY (Developer_ID) REFERENCES Developer(Developer_ID)
                        );""")


def create_sales_info_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Sales_Info CASCADE;")
        cur.execute("""CREATE TABLE Sales_Info (
                            Sale_ID SERIAL PRIMARY KEY,
                            Indexx INT,
                            Region VARCHAR(10),
                            Sales_in_Millions DECIMAL(10, 2),
                            FOREIGN KEY (Indexx) REFERENCES Video_Game(Indexx)
                        );""")

def create_global_sales_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Global_Sales CASCADE;")
        cur.execute("""CREATE TABLE Global_Sales (
                            Indexx INT PRIMARY KEY,
                            Global_Sales_in_Millions DECIMAL(10, 2),
                            FOREIGN KEY (Indexx) REFERENCES Video_Game(Indexx)
                        );
                        """)


def insert_game_table(cur, row):
    cur.execute(
        "SELECT COUNT(*) FROM video_game WHERE Indexx = '{}'".format(row["Indexx"]))
    count = cur.fetchone()[0]
    if count == 0:
        get_developer_id = "SELECT Developer_id FROM developer WHERE Developer = '{}'".format(
            row['Developer'].replace("'", ''))
        cur.execute(get_developer_id)
        developer_id = cur.fetchone()[0]
        get_publisher_id = "SELECT Publisher_id FROM publisher WHERE Publisher = '{}'".format(
            row['Publisher'].replace("'", ''))
        cur.execute(get_publisher_id)
        publisher_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO video_game (Indexx, Name, Platform, Year_of_Release, Genre, "
            "Publisher_ID, Developer_ID, Critic_Score, Critic_Count, User_Score, User_Count, Rating) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (row['Indexx'], row['Name'], row['Platform'].replace("'", ''), row['Year_of_Release'], row['Genre'],
             publisher_id, developer_id, row['Critic_Score'], row['Critic_Count'], row['User_Score'],
             row['User_Count'], row['Rating']))

def insert_developer_table(cur, row):
    cur.execute(
        "SELECT COUNT(*) FROM Developer WHERE Developer = '{}'".format(row['Developer'].replace("'", '')))
    count = cur.fetchone()[0]
    if count == 0:
        max_existing_id = "SELECT MAX(Developer_id) FROM Developer"
        cur.execute(max_existing_id)
        max_existing_id = cur.fetchone()[0]
        if max_existing_id is None:
            max_existing_id = 0
        else:
            max_existing_id += 1
        cur.execute(
            "INSERT INTO Developer (Developer_id, Developer) VALUES (%s, %s)",
            (max_existing_id, row['Developer'].replace("'", '')))

def insert_publisher_table(cur, row):
    cur.execute(
        "SELECT COUNT(*) FROM Publisher WHERE Publisher = '{}'".format(row['Publisher'].replace("'", '')))
    count = cur.fetchone()[0]
    if count == 0:
        max_existing_id = "SELECT MAX(Publisher_id) FROM Publisher"
        cur.execute(max_existing_id)
        max_existing_id = cur.fetchone()[0]
        if max_existing_id is None:
            max_existing_id = 0
        else:
            max_existing_id += 1
        cur.execute(
            "INSERT INTO Publisher (Publisher_id, Publisher) VALUES (%s, %s)",
            (max_existing_id + 1, row['Publisher'].replace("'", '')))

def insert_sales_info_table(cur, row):
    cur.execute(
        "SELECT COUNT(*) FROM Sales_info WHERE Indexx = '{}'".format(row['Indexx']))
    count = cur.fetchone()[0]
    if count == 0:
        for region in ['NA', 'EU', 'JP', 'Other']:
            cur.execute(
                "INSERT INTO Sales_info (Indexx, Region, Sales_in_millions) VALUES (%s, %s, %s)",
                (row['Indexx'], region, row[f'{region}_Sales']))

def insert_global_sales_table(cur, row):
    cur.execute(
        "SELECT COUNT(*) FROM Global_sales WHERE Indexx = '{}'".format(row['Indexx']))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(
            "INSERT INTO Global_sales (Indexx, Global_Sales_in_Millions) VALUES (%s, %s)",
            (row['Indexx'], row['Global_Sales']))


if __name__ == "__main__":
    data = read_csv()
    conn = db_connect()
    create_functions = [create_game_table, create_developer_table, create_publisher_table,
                        create_sales_info_table, create_global_sales_table]
    insert_functions = [insert_developer_table, insert_publisher_table, insert_game_table,
                        insert_sales_info_table, insert_global_sales_table]
    for i in create_functions:
        i(conn)
    start_time = time.time()
    num = 0
    for index, row in data.iterrows():
        with conn:
            cur = conn.cursor()
        if int(index) % 1000 == 0:
            elapsed_time = time.time() - start_time
            print(f"Imported {index}, Elapsed Time: {round(elapsed_time, 2)} seconds")
            start_time = time.time()

        for func in insert_functions:
            func(cur, row)

        num = index

    elapsed_time = time.time() - start_time
    print(f"Imported {num}, Elapsed Time: {round(elapsed_time, 2)} seconds")
