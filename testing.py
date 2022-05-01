import psycopg2
import os

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
print("Table created successfully")

conn.commit()
conn.close()