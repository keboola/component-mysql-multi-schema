from mysql_connect.client import Client

c = Client('localhost', 3308,'root','college')

res = c.get_schemas_by_pattern('northwind*')

print(res)