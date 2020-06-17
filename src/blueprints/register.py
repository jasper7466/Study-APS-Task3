from database import db
from flask import (
    Blueprint,
    request
)
from flask.views import MethodView
from services.register import (
    RegisterService,
    RegistrationFailedError
)

bp = Blueprint('register', __name__)


class RegisterView(MethodView):
    def post(self, account=None):
        """
        Обработчик POST-запроса на регистрацию пользователя.
        :return:
        """
        request_json = request.json

        with db.connection as con:
            service = RegisterService(con)
            try:
                new_user = service.register(request_json)
            except RegistrationFailedError:
                return '', 409
            else:
                return new_user, 201, {'Content-Type': 'application/json'}


bp.add_url_rule('', view_func=RegisterView.as_view('register'))
