import psycopg2
import os
import sys
# sys.path.append('src/python/tools/db2graph/')
from src.python.tools.db2graph.t2g import connect_to_db

conn = psycopg2.connect(database = "postgres", user = "postgres", password = "postgres", 
    host = os.environ.get("POSTGRES_HOST"), port = os.environ.get("POSTGRES_PORT"))
print("Opened database successfully")

cur = conn.cursor()
cur.execute('''CREATE TABLE COMPANY
      (ID INT PRIMARY KEY     NOT NULL,
      NAME           TEXT    NOT NULL,
      AGE            INT     NOT NULL,
      ADDRESS        CHAR(50),
      SALARY         REAL);''')

conn.commit()
print("Table created successfully")

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (1, 'Paul', 32, 'California', 20000.00 )")

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (2, 'Allen', 25, 'Texas', 15000.00 )")

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (3, 'Teddy', 23, 'Norway', 20000.00 )")

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 )")

print("Records created successfully")
conn.commit()

conn.close()

conn = connect_to_db(db_server = "postgre-sql", db_name = "postgres", db_user = "postgres", 
                db_password = "postgres", db_host = os.environ.get("POSTGRES_HOST"))
cur = conn.cursor()

cur.execute("SELECT id, name, address, salary  from COMPANY")
rows = cur.fetchall()
for row in rows:
   print ("ID = ", row[0])
   print ("NAME = ", row[1])
   print ("ADDRESS = ", row[2])
   print ("SALARY = ", row[3], "\n")

conn.close()