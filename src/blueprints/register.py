from database import db
from flask import (
    Blueprint,
    request,
    jsonify
)
from flask.views import MethodView
from services.register import (
    RegisterService,
    RegistrationFailedError
)

bp = Blueprint('register', __name__)


class RegisterView(MethodView):
    def post(self):
        """
        Обработчик POST-запроса на регистрацию пользователя.

        :return: параметры созданного пользователя
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        with db.connection as con:
            service = RegisterService(con)
            try:
                new_user = service.register(data)
            except RegistrationFailedError:
                return '', 409
            else:
                return jsonify(new_user), 201, {'Content-Type': 'application/json'}


bp.add_url_rule('', view_func=RegisterView.as_view('register'))
