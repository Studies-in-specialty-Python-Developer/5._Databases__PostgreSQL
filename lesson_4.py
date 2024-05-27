from python_postgrsql import create_connection, execute_read_query, execute_query

if __name__ == "__main__":
    connection = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    operation_query = """
        CREATE OR REPLACE VIEW customer_orders AS 
        SELECT orders.order_id, orders.customer_id, orders.order_date 
        FROM orders;
    """
    final_set = execute_query(connection, operation_query)
    print('\n1. CREATE VIEW customer_orders\n\n', final_set, '\n')
    operation_query = """
        SELECT * 
        FROM customer_orders
        LIMIT 5;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)

    operation_query = """
        CREATE OR REPLACE VIEW customer_order_details AS 
        SELECT orders.order_id, orders.customer_id, order_details.product_id, order_details.quantity 
        FROM orders 
        JOIN order_details 
        ON orders.order_id = order_details.order_id;
    """
    final_set = execute_query(connection, operation_query)
    print('\n2. CREATE VIEW customer_order_details\n\n', final_set, '\n')
    operation_query = """
        SELECT * 
        FROM customer_order_details 
        WHERE customer_id = 'ALFKI'
        LIMIT 5;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)

    operation_query = """
        CREATE OR REPLACE VIEW customer_order_total AS 
        SELECT orders.order_id, orders.customer_id, 
            (SELECT SUM(order_details.quantity * order_details.unit_price) 
            FROM order_details 
            WHERE orders.order_id = order_details.order_id) AS order_total 
        FROM orders;
    """
    final_set = execute_query(connection, operation_query)
    print('\n3. CREATE VIEW customer_order_total\n\n', final_set, '\n')
    operation_query = """
        SELECT customer_id, SUM(order_total) AS total 
        FROM customer_order_total 
        GROUP BY customer_id
        LIMIT 5;
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)

    operation_query = """
        DROP VIEW IF EXISTS customer_orders;
        DROP VIEW IF EXISTS customer_order_total;
    """
    final_set = execute_query(connection, operation_query)
    print('\n4. DROP VIEW customer_orders, customer_order_total\n\n', final_set, '\n')

    operation_query = """
        DROP TABLE IF EXISTS tmp_customers;
    """
    final_set = execute_query(connection, operation_query)
    print(final_set)
    operation_query = """
        SELECT *
        INTO tmp_customers
        FROM customers;
    """
    final_set = execute_query(connection, operation_query)
    print('\n5.1. CREATE TABLE tmp_customers\n\n', final_set, '\n')
    operation_query = """
        CREATE OR REPLACE FUNCTION update_customer_fax() RETURNS void AS $$
            UPDATE tmp_customers
            SET fax = 'do not use fax'
            WHERE fax is Null
        $$ LANGUAGE SQL;
    """
    final_set = execute_query(connection, operation_query)
    print('\n5.2. CREATE FUNCTION update_customer_fax\n\n', final_set, '\n')
    operation_query = """
        SELECT update_customer_fax();
    """
    final_set = execute_query(connection, operation_query)
    print('\n5.3. UPDATE tmp_customers Fax\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION update_customer_fax(old_value varchar(24), new_value varchar(24)) RETURNS void AS $$
            UPDATE tmp_customers
            SET fax = new_value
            WHERE fax = old_value
        $$ LANGUAGE SQL;
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT update_customer_fax('do not use fax', 'no fax')
    """
    final_set = execute_query(connection, operation_query)
    print('\n5.4. CREATE FUNCTION update_customer_fax(old_value, new_value)\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION get_number_of_goods() RETURNS int AS $$
        BEGIN
            RETURN SUM(units_in_stock)
            FROM products;
        END
        $$ LANGUAGE plpgsql
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT get_number_of_goods()
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)
    print('\n6. FUNCTION number_of_goods by plpgsql\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION get_start_stop_work_dates(OUT start_data DATE, OUT stop_data DATE) AS $$
        BEGIN
            SELECT MIN(orders.order_date), MAX(orders.order_date)
            INTO start_data, stop_data
            FROM orders;
        END
        $$ LANGUAGE plpgsql
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT *
        FROM get_start_stop_work_dates();
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)
    print('\n7. FUNCTION get_start_stop_work_dates by plpgsql\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION replace_substring(string CHARACTER, sub_str_old CHARACTER, sub_str_new CHARACTER, OUT result_string CHARACTER) AS $$
        BEGIN
            result_string = regexp_replace(string, concat('(', sub_str_old, ')', '+'), sub_str_new, 'g');
        END
        $$ LANGUAGE plpgsql
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT *
        FROM replace_substring('asdf-hhjg-trem---hhgfd-j', '-', '?');
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)
    print('\n8. FUNCTION replace_substring by plpgsql\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION get_all_products_of_category(category_id int) RETURNS SETOF products AS $$
        BEGIN
            RETURN QUERY
            SELECT * 
            FROM products
            WHERE products.category_id = get_all_products_of_category.category_id;
        END
        $$ LANGUAGE plpgsql
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT *
        FROM get_all_products_of_category(2);
    """
    final_set = execute_read_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)
    print('\n9. FUNCTION get_all_products_of_category by plpgsql\n\n', final_set, '\n')

    operation_query = """
        CREATE OR REPLACE FUNCTION fibo(n int) RETURNS int AS $$
        DECLARE
            counter int := 0;
            i int = 0;
            j int = 1;
        BEGIN
            IF n < i THEN
                RETURN 0;
            END IF;
            WHILE counter < n
            LOOP
                counter = counter + 1;
                SELECT j, i + j INTO i, j;
            END LOOP;
            RETURN i;
        END;
        $$ LANGUAGE plpgsql
    """
    final_set = execute_query(connection, operation_query)
    operation_query = """
        SELECT *
        FROM fibo(10);
    """
    final_set = execute_query(connection, operation_query)
    if final_set:
        for record in final_set:
            print(record)
    else:
        print(final_set)
    print('\n10. FUNCTION fibonacci by plpgsql\n\n', final_set, '\n')