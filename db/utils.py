import mysql.connector


def get_mysql_cnx(user='root', password='houspider', host='localhost', database='house'):
    return mysql.connector.connect(user=user, password=password, host=host, database=database)


def is_row_exist(col, val, table_name, cur):
    cur.execute(f"SELECT {col} from {table_name} WHERE {col}=%s;", (val,))
    return cur.rowcount > 0


def update_table(val_map, where_clause, table_name, cur):
    column_list = list(val_map.keys())
    value_list = [val_map[x] for x in column_list]
    set_clause = ','.join([f'{k}=%s' for k in column_list])
    query = f"""
        UPDATE {table_name} 
        SET
            {set_clause}
        WHERE
            {where_clause};
    """
    cur.execute(query, tuple(value_list))
    return cur.rowcount


def insert_table(val_map, table_name, cur, on_duplicate_update_val_map=None):
    column_list = list(val_map.keys())
    value_list = [val_map[x] for x in column_list]
    
    param_list = value_list
    
    query = f"""
        INSERT INTO {table_name} ({','.join(column_list)})
        VALUES ({','.join(["%s"] * len(value_list))})
        """
    if on_duplicate_update_val_map is not None:
        update_column_list = list(on_duplicate_update_val_map.keys())
        update_value_list = [on_duplicate_update_val_map[x] for x in update_column_list]
        param_list += update_value_list
        
        on_duplicate_update_clause = ','.join([f'{k}=%s' for k in update_column_list])
        query += f"""
        ON DUPLICATE KEY UPDATE
           {on_duplicate_update_clause}
        """
    cur.execute(f'{query};', tuple(param_list))
    return cur.rowcount
