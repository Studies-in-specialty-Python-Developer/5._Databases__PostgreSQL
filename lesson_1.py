from python_postgrsql import create_connection, execute_query, execute_read_query

if __name__ == "__main__":
    connection = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    sql_query = "SELECT * FROM customers;"

    query_result = execute_read_query(
        connection=connection,
        query=sql_query)

    print(query_result)
