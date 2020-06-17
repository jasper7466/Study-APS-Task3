import sqlite3 as sqlite
from exceptions import ServiceError
from werkzeug.security import generate_password_hash


class RegisterServiceError(ServiceError):
    service = 'register'


class RegistrationFailedError(RegisterServiceError):
    pass


class RegisterService:
    def __init__(self, connection):
        self.connection = connection

    def register(self, new_user):
        """
        Метод для регистрации нового пользователя
        :param new_user: параметры нового пользователя
        :return new_user: новый пользователь
        """
        # Замена открытого пароля на его хеш
        password_hash = generate_password_hash(new_user['password'])
        new_user['password'] = password_hash

        # Создание списков ключей и значений
        keys = ', '.join(f'{key}' for key in new_user.keys())
        values = ', '.join(f'"{value}"' for value in new_user.values())

        # Попытка записи в БД
        try:
            self.connection.execute('PRAGMA foreign_keys = ON')
            cur = self.connection.execute(f'INSERT INTO user ({keys}) VALUES ({values})')
            instance_id = cur.lastrowid
        except sqlite.IntegrityError:
            self.connection.rollback()
            raise RegistrationFailedError()
        else:
            # Подготовка ответа
            new_user.pop('password')
            new_user['id'] = instance_id

        return new_user
