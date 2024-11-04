from menus import *
from utils import *
from queries import *
from graphics import *
from load_data import *
from database import PostgresSQL
# Define functions or variables
def main_menu():
    db_instance = PostgresSQL(
        user='postgres',
        password='Neo_900110',
        host='localhost',
        port='5432',
        database='FCED',
        schema='fires'
    )
    db_instance.test_connection()
    option = None
    options = [
        {'index': 0, 'text': 'Exit'},
        {'index': 1, 'text': 'Insert data'},
        {'index': 2, 'text': 'Update data'},
        {'index': 3, 'text': 'Delete data'},
        {'index': 4, 'text': 'Search data'},
        {'index': 5, 'text': 'Load data from excel'},
        {'index': 6, 'text': 'Queries'},
        {'index': 7, 'text': 'Graphics'},
    ]

    while option != 0:
        option = utils.print_menu(options, "main menu", "Please select one of the previous options")
        if option == 1:
            insert_menu(db_instance)
        elif option == 2:
            update_menu(db_instance)
        elif option == 3:
            delete_menu(db_instance)
        elif option == 4:
            search_menu(db_instance)
        elif option == 5:
            load_wildfires(db_instance)
        elif option == 6:
            queries(db_instance)
        elif option == 7:
            graphics(db_instance)
    db_instance.close_connection()
    exit(0)

# Main block
if __name__ == "__main__":
    main_menu()