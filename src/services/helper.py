import sqlite3 as sqlite


def insert(table, data, connection):
    """
    Функция для записи данных в таблицу БД через словарь, когда названия его ключей
    совпадают с названиями полей таблицы.

    :param table: имя таблицы
    :param data: словарь с записываемыми данными
    :param connection: соединение с БД
    :return: идентификатор записи
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


def update(table, data, id, connection, ref_id=None):
    """
    Функция для обновления данных в таблице БД через словарь, когда названия его ключей
    совпадают с названиями полей таблицы.

    :param table: имя таблицы
    :param data: словарь с новыми данными
    :param id: идентификатор обновляемой записи
    :param connection: соединение с БД
    :param ref_id: поле, по которому проверяется условие (по умолчанию - "id")
    :return: результат выполнения (True/False)
    """
    records = ', '.join(f'{key} = {quotes(value) if value else "NULL"}' for key, value in data.items())
    print(records)
    try:
        connection.execute('PRAGMA foreign_keys = ON')
        connection.execute(f'UPDATE {table} SET {records} WHERE {quotes(ref_id) if ref_id else "id"} = {id}')
    except sqlite.IntegrityError:
        connection.rollback()
        return False
    else:
        return True


def delete(table, id, connection):
    """
    Функция для удаления сущности из таблицы БД по идентификатору.

    :param table: имя таблицы
    :param id: идентификатор записи
    :param connection: соединение с БД
    :return: результат выполнения (True/False)
    """
    try:
        #connection.execute('PRAGMA foreign_keys = ON')
        connection.execute(f'DELETE FROM {table} WHERE id = {id}')
    except sqlite.IntegrityError:
        connection.rollback()
        return False
    else:
        return True


def quotes(value):
    """
    Вспомогательная утилита, оборачивающая переменную в кавычки
    для формирования запросов к БД.

    :param value: значение
    :return: "значение"
    """
    return f'"{value}"'
