from flask import (
    Blueprint,
    request,
    jsonify
)
from flask.views import MethodView
from database import db
from services.decorators import auth_required
from services.transactions import (
    TransactionsService,
    TransactionDoesNotExistError
)

bp = Blueprint('transactions', __name__)


class TransactionsView(MethodView):
    """
    Класс, представляющий часть API, отвечающую за работу с операциями.
    Каждый метод класса реализует обработку одного из запросов, результатом выполнения
    которого является сформированый ответ в формате JSON + код HTTP-ответа.
    """
    @auth_required
    def patch(self, transaction_id, user):
        """
        Обработчик PATCH-запроса на редактирование операции.
        :param transaction_id: идентификатор операции
        :param user: параметры авторизации
        :return response: сформированный ответ
        """
        data = request.json
        with db.connection as con:
            service = TransactionsService(con)
            response = service.patch(transaction_id, user['user_id'], data)
            # Проверка на существование операции и принадлежность её пользователю
            try:
                owner = service.is_owner(transaction_id, user['user_id'])
            except TransactionDoesNotExistError:
                return '', 404
            else:
                if not owner:
                    return '', 403


bp.add_url_rule('/<int:transaction_id>', view_func=TransactionsView.as_view('transactions'))