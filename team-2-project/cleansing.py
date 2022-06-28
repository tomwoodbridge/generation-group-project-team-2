import csv
import re

# Reading the content from csv file
def read_csv():
    with open('src/data/chesterfield_08-03-2021_22-37-00.csv') as file:
        reader = csv.reader(file)
        content = []
        for row in reader:
            i = 0
            new_dict = {}
            new_dict['purchase_date'] = row[i]
            new_dict['store_location'] = row[i+1]
            new_dict['order_items'] = row[i+3]
            new_dict['payment_method'] = row[i+4]
            new_dict['total_spent'] = row[i+5]
            content.append(new_dict)
        return content

content = read_csv()


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
    
basket = create_basket(content)



# # Extracts basket data and stores in a list of order lists which contains a dictionary for each basket entry per order
# def create_basket():
#     basket = []
#     list_split = []
#     for dict in content:
#         for key, value in dict.items():
#             if key == 'order_items':
#                 split_values = value.split(',')
#                 # print(split_values)
#                 list_split.append(split_values)
#                 # print(list_split)
#     for item in list_split:
#         i = 0
#         order = []
#         while i != len(item):
#             new_dict = {}
#             new_dict['size'] = item[i]
#             new_dict['product'] = item[i+1]
#             new_dict['product_price'] = item[i+2]
#             i += 3
#             order.append(new_dict)
#         else:
#             pass
#         basket.append(order)
#     return basket

# basket = create_basket()

# Deleting order's items
def delete_orderitems(content):
    for dict in content:
        del dict['order_items']
    return content

transaction = delete_orderitems(content)
print(transaction)