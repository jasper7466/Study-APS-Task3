from exceptions import ServiceError
from werkzeug.security import generate_password_hash
from services.helper import insert


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

        # Запись в БД
        user_id = insert('user', new_user, self.connection)
        if user_id is None:
            raise RegistrationFailedError()
        else:
            # Подготовка ответа
            new_user.pop('password')
            new_user['id'] = user_id
            return new_user
