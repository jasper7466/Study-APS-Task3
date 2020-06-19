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
    TransactionDoesNotExistError,
    TransactionAccessDeniedError,
    TransactionPatchError,
    MissingImportantFields,
    NegativeValue,
    CategoryNotExists,
    OtherUserCategory,
    TransactionAddingFailedError
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
            try:
                response = service.patch(transaction_id, user['id'], data)
            except TransactionDoesNotExistError:
                return '', 404
            except TransactionAccessDeniedError:
                return '', 403
            except TransactionPatchError:
                return '', 500
            else:
                return response, 200

    @auth_required
    def post(self, user):
        """
        Обработчик POST-запроса на добавление новой операции
        :return new_transaction: поля новой операции
        """
        request_json = request.json
        request_json['user_id'] = user['id']

        with db.connection as connection:
            service = TransactionsService(connection)

            try:
                new_transaction = service.add_transaction(request_json)
            except MissingImportantFields:
                return '', 400
            except CategoryNotExists:
                return '', 404
            except OtherUserCategory:
                return '', 403
            except NegativeValue:
                return '', 400
            except TransactionAddingFailedError:
                return '', 409
            else:
                return new_transaction, 201


bp.add_url_rule('/<int:transaction_id>', view_func=TransactionsView.as_view('transactions'))
