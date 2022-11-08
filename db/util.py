import db.connector


def get_mysql_cnx(user='root', password='houspider', host='localhost', database='house'):
    return db.connector.connect(user, password, host, database)
