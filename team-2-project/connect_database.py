import psycopg2
from psycopg2.extensions import AsIs
import csv 
from cleansing import *

# Postgres connection 
connection = psycopg2.connect(
    host="localhost",
    database="project",
    user="admin",
    password="admin")

mycursor=connection.cursor()

# Creating schema and Table
mycursor.execute("CREATE SCHEMA IF NOT EXISTS project_schema")

# Creating tables
mycursor.execute("drop table if exists project_schema.transaction cascade") 
mycursor.execute('''CREATE TABLE  project_schema.transaction (
                    ID SERIAL PRIMARY KEY,
                    purchase_date VARCHAR(255),
                    store_location VARCHAR(255),
                    payment_method VARCHAR(255),
                    total_spent FLOAT
                    );''')

mycursor.execute("drop table if exists project_schema.basket cascade") 
mycursor.execute(''' CREATE TABLE project_schema.basket (
                    item_id SERIAL PRIMARY KEY,
                    transaction_id INT REFERENCES project_schema.transaction(ID),
                    size VARCHAR(128),
                    product VARCHAR(255) NOT NULL,
                    product_price FLOAT NOT NULL
                    );''')
print("Tables created successfully")

for item in transaction:
    columns = item.keys()
    values = item.values()
    cur = connection.cursor()
    sql = """INSERT INTO project_schema.transaction(%s) VALUES %s;"""
    cur.execute(sql, (AsIs(', '.join(columns)),tuple(values)))

for item in basket:
    for dictionary in item:
        cur = connection.cursor()
        cur.execute(f"""INSERT INTO project_schema.basket (transaction_id, size, product, product_price) 
                    VALUES ('{basket.index(item)+1}', '{dictionary['size']}', '{dictionary['product']}', '{dictionary['product_price']}');""")

connection.commit()
connection.close()
