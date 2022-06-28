import json
import csv
import boto3
import os
import psycopg2
from dotenv import load_dotenv
import sys
import datetime
import psycopg2.extras
import uuid

    
def read_csv(csv_data):
        content = []
        for row in csv_data:
            i = 0
            new_dict = {}
            new_dict['transaction_id'] = str(uuid.uuid4())
            new_dict['purchase_date'] = row[i]
            new_dict['store_location'] = row[i+1]
            new_dict['order_items'] = row[i+3]
            new_dict['total_spent'] = row[i+4]
            new_dict['payment_method'] = row[i+5]
            content.append(new_dict)

        return content
    
    
     
def create_basket(content):
    mini_basket = []
    for dict in content:
        for key, value in dict.items():
            if key == 'order_items':
                split_value = value.split(',')
                mini_basket.append(split_value)
    basket = []
    for item in mini_basket:
        order = []
        for product in item:
            if product.count('-') == 1:
                clean_product = product.split('-')
                if 'Large' in clean_product[0]:
                    replace_large = clean_product[0].replace('Large', ' ')
                    clean_product[0] = replace_large
                    clean_product.insert(0, 'Large')

                elif 'Regular' in clean_product[0]:
                    replace_regular = clean_product[0].replace('Regular', ' ')
                    clean_product[0] = replace_regular
                    clean_product.insert(0, 'Regular')

                else:
                    clean_product.insert(0, 'Regular')
                order.append(clean_product)
            elif product.count('-') == 2:
                clean_product = product.rsplit('-', 1)
                if 'Large' in clean_product[0]:
                    replace_large = clean_product[0].replace('Large', ' ')
                    clean_product[0] = replace_large
                    clean_product.insert(0, 'Large')

                elif 'Regular' in clean_product[0]:
                    replace_regular = clean_product[0].replace('Regular', ' ')
                    clean_product[0] = replace_regular
                    clean_product.insert(0, 'Regular')

                else:
                    clean_product.insert(0, 'Regular')
                order.append(clean_product)
        basket.append(order)
    return basket

# Deleting order's items
def delete_orderitems(content):
    for dict in content:
        del dict['order_items']
    
    return content
    

def get_transactions(csv_data, basket):
    transactions = []
    for row, order in zip(csv_data, basket):
        new_dict = { 
            'transaction_id' : row['transaction_id'],
            'purchase_date': datetime.datetime.strptime(row['purchase_date'], '%d/%m/%Y %H:%M'),
            'store_location': row['store_location'],
            'payment_method': row['payment_method'],
            'total_spent': row['total_spent'],
            'basket': order
        }
        transactions.append(new_dict)
    return transactions

def load_transaction(transaction, basket_list):
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")
    
    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600) # Length of time access is granted
    except Exception as e:
        print(e)
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as e:
        print(e)
            
    with conn.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, """
                INSERT INTO transaction (transaction_id, purchase_date, store_location, payment_method, total_spent) VALUES %s;
            """, [(
                t['transaction_id'],
                t['purchase_date'],
                t['store_location'],
                t['payment_method'],
                t['total_spent']
            )for t in transaction],
            template='(%s,%s,%s,%s,%s)')
            
        for t in transaction:
            psycopg2.extras.execute_values(cursor, """
                    INSERT INTO basket(transaction_id, product_size, product, product_price) VALUES %s;
                """, [(
                    t['transaction_id'],
                    b[0],
                    b[1],
                    b[2]
                ) for b in t['basket']],
                template='(%s,%s,%s,%s)')
            
        conn.commit()
   

def handle(event, context):
    
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket = bucket, Key = key)
    data = s3_object['Body'].read().decode('utf-8')
    
    # read CSV
    csv_data = csv.reader(data.splitlines())
    
    #Assign header and remove unwanted columns
    clean_data = read_csv(csv_data)
    
    #Normalise data by creating a basket table
    basket = create_basket(clean_data)
    
    #Remove items to make transactions table
    transactions = get_transactions(clean_data, basket)
    
    #Load into RedShift...
    load_transaction(transactions, basket)
