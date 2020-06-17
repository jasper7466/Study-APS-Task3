import sqlite3 as sqlite


def insert(table, data, connection):
    """
    Функция для записи данных в таблицу БД через словарь, когда названия его ключей
    совпадают с названиями полей таблицы.
    :param table: имя таблицы
    :param data: словарь с записываемыми данными
    :param connection: соединение с БД
    :return instance_id: идентифкатор записи
    """
    # Создание списков ключей и значений
    keys = ', '.join(f'{key}' for key in data.keys())
    values = ', '.join(f'"{value}"' for value in data.values())

    # Попытка записи в БД
    try:
        connection.execute('PRAGMA foreign_keys = ON')
        cur = connection.execute(f'INSERT INTO {table} ({keys}) VALUES ({values})')
        instance_id = cur.lastrowid
    except sqlite.IntegrityError:
        connection.rollback()
        return None
    else:
        return instance_id
