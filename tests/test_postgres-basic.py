import os
import psycopg2
import random
import sys
sys.path.append('src/python/tools/db2graph/') # moving to the parent directory
from db2graph import connect_to_db

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

    def test_connect_to_db(self):
        """
        Basic connecter to db test. Just checking if connection established
        and corrected values are fetched
        """
        # Filling database with data for testing
        conn = psycopg2.connect(database = self.database, user = self.user, password = self.password,
            host = self.host, port = self.port)
        
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