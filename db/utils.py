import mysql.connector


def get_mysql_cnx(user='root', password='houspider', host='localhost', database='house'):
    return mysql.connector.connect(user=user, password=password, host=host, database=database)


def is_row_exist(col, val, table_name, cur):
    cur.execute(f"SELECT {col} from {table_name} WHERE {col}={val};")
    return cur.rowcount > 0


def update_table(val_map, where_clause, table_name, cur):
    set_clause = ','.join([f'{k}={val_map[k]}' for k in val_map])
    query = f"""
        UPDATE {table_name} 
        SET 
            {set_clause}
        WHERE
            {where_clause};
    """
    cur.execute(query)
    return cur.rowcount


def insert_table(val_map, table_name, cur, on_duplicate_update_val_map=None):
    column_list = list(val_map.keys())
    value_list = [str(val_map[x]) for x in column_list]
    on_duplicate_update_clause = ','.join([f'{k}={val_map[k]}' for k in on_duplicate_update_val_map])
    query = f"""
        INSERT INTO {table_name} ({','.join(column_list)})
        VALUES ({','.join(value_list)})
        """
    if on_duplicate_update_val_map is not None:
        query += f"""
        ON DUPLICATE KEY UPDATE
           {on_duplicate_update_clause}
        """
    cur.execute(f'{query};')
    return cur.rowcount
