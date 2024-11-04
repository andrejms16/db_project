import re
import unicodedata
from datetime import datetime

def print_menu(options, menu_title, menu_prompt):
    print('')
    print("-*" * 40)
    print(menu_title.upper())
    print("-*" * 40)
    for option in options:
        print(f'[{option['index']}] {option["text"]}')
    print("-*" * 40)
    try:
        option = int(input(menu_prompt+"   "))
        if option < len(options):
            return option
        else:
            print('\033[31mInvalid option, please try again\033[0m')
            return None
    except ValueError:
        print("\033[31mPlease enter a valid number, try again\033[0m")
        return None


def text_standardize(text):
    text = str(text).upper()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'[-_.,;:!@#?(){}[\]*/\\]', '', text)
    return text


def get_timestamp(date):
    dt = datetime.strptime(date, '%d/%m/%Y %H:%M')
    return dt.timestamp()
