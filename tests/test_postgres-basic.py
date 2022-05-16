import os
import psycopg2
import random
import sys
from pathlib import Path
sys.path.append('src/python/tools/db2graph/') # moving to the parent directory
from db2graph import connect_to_db, entity_node_to_uuids

class TestConnector():
    database = "postgres"
    user = "postgres"
    password = "postgres"
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")

    @classmethod
    def set_up(self):
        pass
    
    @classmethod
    def tear_down(self):
        pass

    def fill_db(self):
        """
        Filling the database with data for testing things
        """
        conn = psycopg2.connect(database = self.database,
                                user = self.user,
                                password = self.password,
                                host = self.host,
                                port = self.port)
        cur = conn.cursor()

        # Create two tables - First Customers and second Orders
        cur.execute('''CREATE TABLE CUSTOMERS
                        (ID INT PRIMARY KEY NOT NULL,
                        CUSTOMERNAME TEXT NOT NULL,
                        COUNTRY TEXT NOT NULL,
                        PHONE VARCHAR(10) NOT NULL);''')
        conn.commit()
        cur.execute('''CREATE TABLE ORDERS
                        (ID INT PRIMARY KEY NOT NULL,
                        CUSTOMERID INT FOREIGN KEY REFERENCES CUSTOMERS(ID),
                        AMOUNT INT NOT NULL,
                        ITEM TEXT NOT NULL);''')
        conn.commit()

        # Insert some data
        # Inserting Customers
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (1, 'Sofia', 'Spain', '6081237654')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (2, 'Lukas', 'Germany', '6721576540')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (3, 'Rajesh', 'India', '5511234567')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (4, 'Daiyu', 'China', '3211248173')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (5, 'Hina', 'Japan', '6667890001')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (6, 'Lorenzo', 'Italy', '6260001111')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (7, 'Donghai', 'China', '7874561234')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (8, 'Shuchang', 'China', '4041015059')")
        cur.execute(f"INSERT INTO CUSTOMERS (ID,CUSTOMERNAME,COUNTRY,PHONE) \
            VALUES (9, 'Johnny', 'USA', '5647525398')")
        conn.commit()

        # Inserting Orders
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (1, 3, 5, 'fenugreek')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (2, 7, 7, 'soy sauce')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (3, 6, 2, 'oregano')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (4, 1, 3, 'tomato')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (5, 3, 5, 'cumin')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (6, 5, 7, 'soy sauce')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (7, 2, 1, 'eggs')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (8, 9, 3, 'onions')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (9, 4, 3, 'onions')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (10, 5, 15, 'wasabi')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (11, 8, 9, 'rice')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (12, 4, 12, 'chicken breast')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (13, 5, 20, 'salmon')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (14, 6, 11, 'sourdough bread')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (15, 2, 8, 'meatballs')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (16, 9, 2, 'root beer')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (17, 2, 6, 'croissant')")
        cur.execute(f"INSERT INTO ORDERS (ID,CUSTOMERID,AMOUNT,ITEM) \
            VALUES (18, 1, 4, 'taco sauce')")
        conn.commit()

        conn.close()
        return

    def test_connect_to_db(self):
        """
        Basic connecter to db test. Just checking if connection established
        and corrected values are fetched
        """
        # Filling database with data for testing
        conn = psycopg2.connect(database = self.database,
                                user = self.user,
                                password = self.password,
                                host = self.host,
                                port = self.port)
        
        # Create table
        cur = conn.cursor()
        cur.execute('''CREATE TABLE COMPANY
            (ID INT PRIMARY KEY     NOT NULL,
            NAME           TEXT    NOT NULL,
            AGE            INT     NOT NULL);''')
        conn.commit()

        # Insert some data
        num_data_to_insert = 5
        self.name = []
        self.age = []
        for i in range(num_data_to_insert):
            self.name.append("name" + str(i))
            self.age.append(random.randint(1, 100))
        
        for i in range(num_data_to_insert):
            cur.execute(f"INSERT INTO COMPANY (ID,NAME,AGE) \
                VALUES ({i}, '{self.name[i]}', {self.age[i]})")
        conn.commit()
        conn.close()

        # Setting the connect function to test
        conn = connect_to_db(db_server = "postgre-sql", db_name = self.database, db_user = self.user,
            db_password = self.password, db_host = self.host)
        cur = conn.cursor()
        cur.execute("SELECT id, name, age from COMPANY")
        rows = cur.fetchall()
        index = 0
        for row in rows:
            assert(row[0] == index)
            assert(row[1] == self.name[index])
            assert(row[2] == self.age[index])
            index += 1
        conn.close()
    
    def test_entity_node_to_uuids(self):
        """
        Testing entity_node_to_uuids function from db2graph.py
        """
        self.fill_db() # Filling database with data for testing

        # Getting all the inputs for the function
        output_dir = Path("output_dir/")
        output_dir.mkdir(parents=True, exist_ok=True)
        db_server = 'postgre-sql'
        conn = psycopg2.connect(database = self.database,
                                user = self.user,
                                password = self.password,
                                host = self.host,
                                port = self.port)
        entity_queries_list = []
        entity_queries_list.append("SELECT DISTINCT customers.customername from customers;")
        entity_queries_list.append("SELECT DISTINCT customers.country from customers;")
        entity_queries_list.append("SELECT DISTINCT orders.item from orders;")

        print(entity_queries_list)
        # Testing the function
        entity_mapping = entity_node_to_uuids(output_dir, conn, entity_queries_list, db_server)
        print(entity_mapping)
        pass