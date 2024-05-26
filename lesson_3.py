from python_postgrsql import create_connection, execute_read_query

if __name__ == "__main__":
    connection = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    operation_query = """
        SELECT employee_id, 
        first_name || ' ' || last_name AS "Full name"
        FROM employees
        WHERE NOT EXISTS (
            SELECT 1
            FROM orders
            WHERE orders.employee_id = employees.employee_id);
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n1. no orders\n\n', final_set, '\n')

    operation_query = """        
        SELECT country
        FROM customers
        EXCEPT
        SELECT country
        FROM suppliers
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n2. EXCEPT\n\n', final_set, '\n')

    operation_query = """        
        SELECT country
        FROM customers
        EXCEPT ALL
        SELECT country
        FROM suppliers
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n2. EXCEPT ALL\n\n', final_set, '\n')
