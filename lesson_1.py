import psycopg2
from psycopg2 import OperationalError


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return result


if __name__ == "__main__":
    connect = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    sql_query = "SELECT * FROM customers;"

    query_result = execute_read_query(
        connection=connect,
        query=sql_query)

    print(query_result)
