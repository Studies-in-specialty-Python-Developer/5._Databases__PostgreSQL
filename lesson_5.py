from python_postgrsql import create_connection, execute_read_query, execute_query

if __name__ == "__main__":
    connection = create_connection(
        db_name='Northwind',
        db_user='postgres',
        db_password='123',
        db_host='localhost',
        db_port='5432')

    operation_query = """
        DROP DATABASE IF EXISTS my_new_db
    """
    final_set = execute_query(connection, operation_query)

    operation_query = """
        CREATE DATABASE my_new_db;
    """
    final_set = execute_query(connection, operation_query)
    print('\n1. CREATE DATABASE\n\n', final_set, '\n')

    my_new_db_connection = create_connection(
        db_name="my_new_db",
        db_user="postgres",
        db_password="123",
        db_host="localhost",
        db_port="5432"
    )

    operation_query = """
        CREATE TABLE publisher
            (id INTEGER,
            title VARCHAR,
            contact VARCHAR);
        CREATE TABLE author
            (id INTEGER,
            name VARCHAR);
        CREATE TABLE book
            (id SERIAL,
            title VARCHAR,
            publisher INTEGER);
        CREATE TABLE author_book
            (author INTEGER,
            book INTEGER);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n2. CREATE TABLES\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE author
            ADD COLUMN first_name VARCHAR;
        ALTER TABLE author
            ADD COLUMN last_name VARCHAR;
        ALTER TABLE author
            ADD COLUMN patronymic VARCHAR;
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n3. ALTER TABLES\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE author
        DROP COLUMN name;
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n4. DROP COLUMN name\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE author
            ALTER COLUMN first_name SET DATA TYPE varchar(50);
        ALTER TABLE author
            ALTER COLUMN last_name SET DATA TYPE varchar(50);
        ALTER TABLE author
            ALTER COLUMN patronymic SET DATA TYPE varchar(50);
        ALTER TABLE book
            ALTER COLUMN title SET DATA TYPE varchar(200);
        ALTER TABLE publisher
            ALTER COLUMN title SET DATA TYPE varchar(100);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n5. SET DATA TYPE\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE author
            DROP COLUMN id;
            ALTER TABLE author
            ADD COLUMN id serial PRIMARY KEY;
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n6. SET author PRIMARY KEY\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE book
            DROP COLUMN id;
            ALTER TABLE book
            ADD COLUMN id serial PRIMARY KEY;
        ALTER TABLE publisher
            DROP COLUMN id;
            ALTER TABLE publisher
            ADD COLUMN id serial PRIMARY KEY;
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n7. SET book, publisher PRIMARY KEY\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE book
            RENAME publisher TO publisher_id;
        ALTER TABLE book
            ADD CONSTRAINT fk_book_publisher_id FOREIGN KEY (publisher_id) REFERENCES publisher(id);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n8. FOREIGN KEY (publisher_id) REFERENCES publisher(id)\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE author_book
            RENAME author TO author_id;
        ALTER TABLE author_book
            RENAME book TO book_id;
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    operation_query = """
        ALTER TABLE author_book
            ADD CONSTRAINT fk_author_book_author_id FOREIGN KEY (author_id) REFERENCES author(id);
        ALTER TABLE author_book
            ADD CONSTRAINT fk_author_book_book_id FOREIGN KEY (book_id) REFERENCES book(id);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n9. FOREIGN KEY (author_id) REFERENCES author(id)\n' +
          'FOREIGN KEY (book_id) REFERENCES book(id)\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE book
            ADD COLUMN price decimal;
        ALTER TABLE book
            ADD CONSTRAINT positive_price CHECK (price >= 0);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n10. ADD COLUMN price\n' +
          'ADD CONSTRAINT positive_price CHECK (price >= 0)\n\n', final_set, '\n')

    operation_query = """
        ALTER TABLE publisher
            ADD COLUMN cooperation_status varchar(11) DEFAULT 'no';
        ALTER TABLE publisher
            ADD CONSTRAINT chk_coop_status CHECK (cooperation_status in ('no', 'yes', 'in_progress'));
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n11. SET cooperation_status varchar(11) DEFAULT no\n\n', final_set, '\n')

    operation_query = """
        INSERT INTO author
        VALUES ('Іван', 'Котляревський', 'Петрович')
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    operation_query = """
        INSERT INTO author
        VALUES 
            ('Остап', 'Вишня'),
            ('Сергій', 'Жадан'),
            ('Оксана', 'Забужко');
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n12. INSERT INTO author\n\n', final_set, '\n')

    operation_query = """
        INSERT INTO publisher
        VALUES 
            ('Ранок', '380571122334'),
            ('Vivat', '380800201102'),
            ('А-БА-БА-ГА-ЛА-МА-ГА', '380686683595'),
            ('Астролябія', '380322762300');
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n13. INSERT INTO publisher\n\n', final_set, '\n')

    operation_query = """
        INSERT INTO book
        VALUES 
            ('Кассандра', 1),
            ('Лісова пісня', 2),
            ('Вишневі усмішки', 3),
            ('Усмішки', 4),
            ('Мисливські усмішки', 1),
            ('Гімн демократичної молоді', 1),
            ('Вогнепальної і ножової', 4),
            ('Інтернат', 3),
            ('Польові дослідження з українського сексу', 3),
            ('Музей покинутих секретів', 2),
            ('Наталка Полтавка', 2),
            ('Енеїда', 2);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n14. INSERT INTO book\n\n', final_set, '\n')

    operation_query = """
        INSERT INTO author_book
        VALUES 
            (2, 1),
            (2, 2),
            (3, 3),
            (3, 4),
            (3, 5),
            (4, 6),
            (4, 7),
            (4, 8),
            (2, 9),
            (1, 10),
            (1, 11),
            (1, 12);
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n15. INSERT INTO author_book\n\n', final_set, '\n')

    operation_query = """
        SELECT 
            author.first_name,
            author.last_name,
            book.title      AS book_title,
            publisher.title AS publ_title
        FROM author
            JOIN author_book ON author.id = author_book.author_id
            JOIN book ON book.id = author_book.book_id
            JOIN publisher ON book.publisher_id = publisher.id
        ORDER BY author.last_name;
    """
    final_set = execute_read_query(my_new_db_connection, operation_query)
    print('\n16. Get author, book, publisher\n\n', final_set, '\n')

    operation_query = """
        UPDATE author
            SET patronymic = 'Вікторович'
        WHERE last_name = 'Жадан' and first_name = 'Сергій';
    """
    final_set = execute_query(my_new_db_connection, operation_query)
    print('\n17. UPDATE data\n\n', final_set, '\n')

    operation_query = """
        UPDATE publisher
        SET cooperation_status = 'in_progress';
    """
    final_set = execute_query(my_new_db_connection, operation_query)

    operation_query = """
        INSERT INTO publisher (title)
            VALUES ('Країна мрій')
        RETURNING *;
    """
    final_set = execute_read_query(my_new_db_connection, operation_query)
    print('\n18. RETURNING\n\n', final_set, '\n')
