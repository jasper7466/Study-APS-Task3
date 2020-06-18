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
    :return response: сформированный ответ
    """
    request_json = request.json
    email = request_json.get('email')
    password = request_json.get('password')
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
