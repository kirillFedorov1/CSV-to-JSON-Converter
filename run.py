import os
import pandas as pd
import json
import sys
import argparse as ap

def main(tables):
    """Преобразовывает CSV файлы в JSON и сохраняет.

    Args:
        tables (list): Список названий таблиц.
    """
    src_path = get_env_value('SRC_PATH')
    tgt_path = get_env_value('TGT_PATH')

    if not tables:
        try:
            tables = [
                f for f in os.listdir(src_path)
                if os.path.isdir(os.path.join(src_path, f))
            ]
        except FileNotFoundError:
            print(f'Директория {src_path} не найдена.')
            return

    for table in tables:
        columns = get_table_columns_from_schema(src_path, table)
        if not columns:
            continue

        dfs = read_csv(src_path, table, columns)
        if not dfs:
            continue
        
        for file, df in dfs.items():
            gen_json(tgt_path, table, file, df)

def get_env_value(key):
    """Получает значение переменной окружения.

    Args:
        key (str): Ключ переменной окружения.

    Returns:
        str: Значение переменной окружения.
    """
    try:
        return os.environ[key]
    except KeyError:
        print(f'Не задана переменная окружения \'{key}\'.')
        sys.exit()

def get_table_columns_from_schema(src_path, table):
    """Получает имена полей из файла schemas.json.

    Args:
        src_path (str): Путь к директории с schemas.json.
        table (str): Название таблицы.

    Returns:
        list: Список полей.
    """
    try:
        with open(os.path.join(src_path, 'schemas.json')) as json_file:
            schemas = json.load(json_file)
    except FileNotFoundError:
        print(f'Schemas.json в директории {src_path} не найден.')
        sys.exit()

    json_columns = schemas.get(table)
    if not json_columns:
        print(f'Для {table} нет описания в schemas.json.')
        return []

    columns = [jc['column_name'] for jc in json_columns]
    return columns

def read_csv(src_path, table, columns):
    """Читает все csv файлы в директории и формирует из них датафреймы.

    Args:
        src_path (str): Путь к директории с файлами.
        table (str): Название таблицы (а также название папки с файлами).
        columns (list): Список полей таблицы.

    Returns:
        dict: Словарь сформированных датафреймов (где ключ - это название файла).
    """
    files = os.listdir(os.path.join(src_path, table))

    if not files:
        print(f'Нет новых данных для {table}.')
        return {}

    dfs = {}
    for file in files:
        full_path = os.path.join(src_path, table, file)
        try:
            dfs[file] = pd.read_csv(full_path, names = columns)
        except Exception as e:
            print(f'Проблема с файлом {file} (таблица {table}). Ошибка: {e}.')

    return dfs

def gen_json(tgt_path, table, file, df):
    """Формирует и сохраняет json файлы.

    Args:
        tgt_path (str): Путь к директории для сохранения файлов.
        table (str): Название таблицы (а также имя папки для сохранения).
        file (str): Название файла.
        df (DataFrame): Датафрейм для таблицы table и файла file.
    """
    table_path = os.path.join(tgt_path, table)
    file_path = os.path.join(table_path, file)

    if not os.path.exists(table_path):
        os.makedirs(table_path)

    df.to_json(file_path, orient='records')

    print(f'Для {table} сформирован json по пути {file_path}.')

if __name__ == '__main__':
    parser = ap.ArgumentParser(description="Преобразование CSV в JSON.")
    parser.add_argument(
        "-t", "--tables",
        type=str,
        help="Список таблиц, разделенных запятыми"
    )
    args = parser.parse_args()
    if args.tables:
        tables = args.tables.split(', ')
        print(tables)
    else:
        tables = []
        print("Обрабатываю все файлы.")

    main(tables)