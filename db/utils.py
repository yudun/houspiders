import mysql.connector


def get_mysql_cnx(user='root', password='houspider', host='localhost', database='house'):
    return mysql.connector.connect(user=user, password=password, host=host, database=database)
