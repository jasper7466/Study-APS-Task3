from exceptions import ServiceError
from werkzeug.security import check_password_hash


class AuthServiceError(ServiceError):
    service = 'auth'


class AuthorizationFailedError(AuthServiceError):
    pass


class UserDoesNotExistError(AuthServiceError):
    pass


class AuthService:
    def __init__(self, connection):
        self.connection = connection

    def login(self, email, password):
        """
        Метод авторизации пользователя.

        :param email: логин (e-mail)
        :param password: пароль
        :return: идентификатор пользователя
        """
        cur = self.connection.execute(f'SELECT id, password FROM user WHERE email = "{email}"')
        user = cur.fetchone()
        if user is None:
            raise UserDoesNotExistError(email)
        if not check_password_hash(user['password'], password):
            raise AuthorizationFailedError(email)
        return user['id']
