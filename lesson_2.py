from python_postgrsql import create_connection, execute_read_query

if __name__ == "__main__":
    connection = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    orders_full_data_sample = """
        SELECT * FROM orders
        LIMIT 10;
    """
    orders_full_sample = execute_read_query(connection, orders_full_data_sample)
    print('\n1. orders_full\n\n', orders_full_sample, '\n')

    orders_some_attributes_sample = """
        SELECT order_id, order_date, shipped_date, ship_city
        FROM orders
        LIMIT 10;
    """
    orders_partly_sample = execute_read_query(connection, orders_some_attributes_sample)
    print('\n2. orders_partly\n\n', orders_partly_sample, '\n')

    operation_query = """
        SELECT product_name, unit_price * units_in_stock
        FROM products
        LIMIT 10;
    """
    total_cost = execute_read_query(connection, operation_query)
    print('\n3. total cost\n\n', total_cost, '\n')

    operation_query = """
        SELECT gcd(1028, 12004);
    """
    gcd = execute_read_query(connection, operation_query)
    print('\n4. greatest common divisor\n\n', gcd, '\n')

    operation_query = """
        SELECT random();
    """
    rand_num = execute_read_query(connection, operation_query)
    print('\n5. random number\n\n', rand_num, '\n')

    operation_query = """
        SELECT DISTINCT country
        FROM customers;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n6.1. DISTINCT country\n\n', final_set, '\n')

    operation_query = """
        SELECT DISTINCT country, city
        FROM customers
        LIMIT 10;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n6.2. DISTINCT country, city\n\n', final_set, '\n')

    operation_query = """
        SELECT COUNT(*)
        FROM suppliers;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n7.1. COUNT(*)\n\n', final_set, '\n')

    operation_query = """
        SELECT COUNT(DISTINCT country)
        FROM employees;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n7.2. COUNT(DISTINCT country)\n\n', final_set, '\n')

    operation_query = """
        SELECT company_name, city, phone
        FROM suppliers
        WHERE country = 'USA';
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n8. WHERE country = USA\n\n', final_set, '\n')

    operation_query = """
        SELECT company_name, city, phone, country
        FROM suppliers
        WHERE country = 'USA' OR country = 'Germany';
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n9. AND, OR   WHERE country = USA OR country = Germany\n\n', final_set, '\n')

    operation_query = """
        SELECT employee_id, last_name, first_name, city, age(birth_date) AS age, age(hire_date) AS time_in_compaty
        from employees
        where (age(birth_date) between  interval '45 years' and interval '60 years') 
        and (age(hire_date) > interval '5 years');
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n10. BETWEEN\n\n', final_set, '\n')

    operation_query = """
        SELECT company_name, city, phone, country
        FROM suppliers
        WHERE country IN ('USA','Germany');
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n11. IN, NOT IN    WHERE country IN (USA, Germany)\n\n', final_set, '\n')

    operation_query = """
        SELECT company_name, country, city
        FROM customers
        ORDER BY country
        LIMIT 10;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n12.1. ORDER BY country\n\n', final_set, '\n')

    operation_query = """
        SELECT company_name, country, city
        FROM customers
        ORDER BY country DESC, city DESC
        LIMIT 10;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n12.2. ORDER BY country DESC, city DESC\n\n', final_set, '\n')

    operation_query = """
        SELECT AVG(unit_price)
        FROM products;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n13.1. MIN, MAX, AVG   AVG(unit_price)\n\n', final_set, '\n')

    operation_query = """
        SELECT MIN(order_date)
        FROM orders
        WHERE ship_country = 'Germany';
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n13.2. MIN, MAX, AVG   MIN(order_date) WHERE ship_country = Germany\n\n', final_set, '\n')

    operation_query = """
        SELECT MAX(birth_date)
        FROM employees
        WHERE city = 'London';
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n13.3. MIN, MAX, AVG   MAX(birth_date) WHERE city = London\n\n', final_set, '\n')

    operation_query = """
        SELECT employee_id, first_name, last_name, city
        FROM employees
        WHERE first_name LIKE 'L%';
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n14. WHERE first_name LIKE L%\n\n', final_set, '\n')

    operation_query = """
        SELECT employee_id, first_name, last_name, city
        FROM employees
        ORDER BY first_name
        LIMIT 10;
    """
    final_set = execute_read_query(connection, operation_query)
    print('\n15. LIMIT 10\n\n', final_set, '\n')

    operation_query = """
        SELECT order_id, order_date, ship_region, ship_name, ship_city
        FROM orders
        WHERE ship_region IS NOT NULL
        LIMIT 20;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n16. Check on NULL\n\n', final_set, '\n')

    operation_query = """
        SELECT ship_country,  COUNT(*) 
        FROM orders
        WHERE freight >= 40
        GROUP BY ship_country
        ORDER BY COUNT(*) DESC;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n17.1. GROUP BY\n\n', final_set, '\n')

    operation_query = """
        SELECT category_id,  SUM(units_in_stock) 
        FROM products
        GROUP BY category_id
        ORDER BY SUM(units_in_stock) DESC;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n17.2. GROUP BY\n\n', final_set, '\n')

    operation_query = """
        SELECT category_id,  SUM(units_in_stock * unit_price) 
        FROM products
        GROUP BY category_id
        HAVING SUM(units_in_stock * unit_price) < 10000;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n18. HAVING\n\n', final_set, '\n')

    operation_query = """
        SELECT country
        FROM suppliers
        UNION
        SELECT country
        FROM employees
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n19.1. UNION\n\n', final_set, '\n')

    operation_query = """
        SELECT country
        FROM suppliers
        INTERSECT
        SELECT country
        FROM customers
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n19.2. INTERSECT\n\n', final_set, '\n')

    operation_query = """
        SELECT country
        FROM customers
        EXCEPT
        SELECT country
        FROM suppliers
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    print('\n19.3. EXCEPT\n\n', final_set, '\n')
