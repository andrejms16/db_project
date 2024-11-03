import psycopg2
import utils

def insert_menu(db_instance):
    table = str(input('Please enter the table name  '))
    values = {}
    value = 0
    columns = db_instance.get_columns(table)
    for column in columns:
        is_serial = db_instance.is_serial(table, column[0])
        is_foreign = db_instance.is_foreign_key(table, column[0])

        if not is_serial:
            if column[1] == 'integer':
                if not is_foreign:
                    value = int(input(f'Enter the value for the column {column[0]} ({column[1]})  '))
                else:
                    value = int(input(f"Enter the value for the column {column[0]} ({column[1]}) - \
                    Foreign Key on table {is_foreign['referenced_table']} \
                    and column {is_foreign['referenced_column']}  "))
            elif column[1] == 'character varying':
                if not is_foreign:
                    value = str(input(f'Enter the value for the column {column[0]} ({column[1]})  '))
                else:
                    value = int(input(f"Enter the value for the column {column[0]} ({column[1]}) - \
                    Foreign Key on table {is_foreign['referenced_table']} \
                    and column {is_foreign['referenced_column']}  "))
            values[column[0]] = value
    if db_instance.insert_data(table, values):
        print(f'The row was inserted into the {table} table')
    return None

def update_menu(db_instance):
    new_value = None
    table = str(input('Please enter the table name  '))
    row_id = int(input(f'Enter the ID [PrimaryKey] of the row you want to update in the table {table}   '))
    print('Row to update:')
    db_instance.query_one(table, ('id', row_id))
    columns = db_instance.get_columns(table)
    options = []
    for index, column in enumerate(columns):
        if not db_instance.is_serial(table, column[0]) and not db_instance.is_primary_key(table, column[0]):
            options.append({'index': index, 'text': column[0]})
    update_index = utils.print_menu(options, f'update table: {table}', 'Enter the column number\
     you want to update')

    if columns[update_index][1] == 'integer':
        new_value = int(input(f'Enter the new value for column {columns[update_index][0]}, item {row_id}    '))
    elif columns[update_index][1] == 'character varying':
        new_value = str(input(f'Enter the new value for column {columns[update_index][0]}, item {row_id}    '))

    if db_instance.update_data(table, {columns[update_index][0]: new_value}, {'id': row_id}):
        print(f'The row with id: {row_id} was updated on the {table} table')
    return None

def delete_menu(db_instance):
    table = str(input('Please enter the table name  '))
    row_id = int(input(f'Enter the ID [PrimaryKey] of the row you want to delete in the table {table}   '))
    print('Row to delete:')
    db_instance.query_one(table, ('id', row_id))
    confirmation = str(input(f'Are you sure you want to delete the row with id: {row_id}? (y/n)  '))
    if confirmation == 'y':
        db_instance.delete_one(table, {'id': row_id})
        print(f'The row with id: {row_id} was deleted on the {table} table')
    return None


def search_menu(db_instance):
    table = str(input('Please enter the table name  '))
    row_id = int(input(f'Enter the ID [PrimaryKey] of the row you want to search in the table {table}   '))
    db_instance.query_one(table, ('id', row_id))
    return None







