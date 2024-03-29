from flask import (
    Blueprint,
    request,
    session
)
from database import db
from services.auth import (
    AuthService,
    AuthorizationFailedError,
    UserDoesNotExistError
)

bp = Blueprint('auth', __name__)


@bp.route('/login', methods=['POST'])
def login():
    """
    Обработчик POST-запроса на авторизацию пользователя.

    :return: сформированный ответ
    """
    data = request.json

    # Проверка на пустое тело запроса
    if not data:
        return '', 400

    email = data.get('email')
    password = data.get('password')
    with db.connection as con:
        service = AuthService(con)
        # Попытка авторизации
        try:
            user_id = service.login(email, password)
        except UserDoesNotExistError:
            return '', 401
        except AuthorizationFailedError:
            return '', 401
        else:
            session['user_id'] = user_id
            return '', 200


@bp.route('/logout', methods=['POST'])
def logout():
    """
    Обработчик POST-запроса на завершение сессии.

    :return: nothing
    """
    session.pop('user_id', None)
    return '', 200
