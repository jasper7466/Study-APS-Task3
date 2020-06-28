from database import db
from flask import (
    Blueprint,
    request,
    jsonify
)
from flask.views import MethodView
from services.decorators import auth_required
from services.transactions import (
    TransactionsService,
    TransactionDoesNotExistError,
    TransactionAccessDeniedError,
    TransactionPatchError,
    MissingRequiredFields,
    NegativeValue,
    CategoryDoesNotExistError,
    CategoryAccessDeniedError,
    TransactionAddingFailedError,
    OtherUserTransaction,
    TransactionNotExists,
    EmptyReportError,
    PageReportNotExist,
    TransactionInvalidPeriodError
)

bp = Blueprint('transactions', __name__)


class TransactionsView(MethodView):
    """
    Класс, представляющий часть API, отвечающую за работу с операциями.
    Каждый метод класса реализует обработку одного из запросов, результатом выполнения
    которого является сформированый ответ в формате JSON + код HTTP-ответа.
    """
    @auth_required
    def post(self, user):
        """
        Обработчик POST-запроса на добавление новой операции.

        :return: параметры новой операции
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        data['user_id'] = user['id']

        with db.connection as connection:
            service = TransactionsService(connection)

            try:
                new_transaction = service.add_transaction(data)
            except MissingRequiredFields:
                return '', 400
            except CategoryDoesNotExistError:
                return '', 404
            except CategoryAccessDeniedError:
                return '', 403
            except NegativeValue:
                return '', 400
            except TransactionAddingFailedError:
                return '', 409
            else:
                return jsonify(new_transaction), 201, {'Content-Type': 'application/json'}

    @auth_required
    def get(self, user):
        """
        Обработчик GET-запроса на получение отчёта по операциям.

        :param user: параметры авторизации
        :return: сформированный ответ
        """
        query_str = request.args
        with db.connection as connection:
            service = TransactionsService(connection)
            try:
                report = service.get_transaction(query_str, user['id'])
            except CategoryDoesNotExistError:
                return '', 404
            except TransactionAccessDeniedError:
                return '', 403
            except EmptyReportError:
                return '', 404
            except PageReportNotExist:
                return '', 404
            except TransactionInvalidPeriodError:
                return '', 400
            else:
                return jsonify(report), 200, {'Content-Type': 'application/json'}


class TransactionView(MethodView):
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
        :return: сформированный ответ
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        with db.connection as con:
            service = TransactionsService(con)
            try:
                response = service.patch_transaction(transaction_id, user['id'], data)
            except TransactionDoesNotExistError:
                return '', 404
            except TransactionAccessDeniedError:
                return '', 403
            except NegativeValue:
                return '', 400
            except TransactionPatchError:
                return '', 500
            else:
                return jsonify(response), 200, {'Content-Type': 'application/json'}
              
    @auth_required
    def delete(self, user, transaction_id):
        """
        Обработчик DELETE-запроса на удаление существующей операции.

        :param user: идентификатор авторизованного пользователя
        :param transaction_id: идентификатор удаляемой операции
        :return: сформированный ответ
        """
        data_to_delete = {
            'user_id': user['id'],
            'transaction_id': transaction_id
        }

        with db.connection as connection:
            service = TransactionsService(connection)

            try:
                service.delete_transaction(data_to_delete)
            except OtherUserTransaction:
                return '', 403
            except TransactionNotExists:
                return '', 404
            else:
                return '', 200


bp.add_url_rule('', view_func=TransactionsView.as_view('transactions'))
bp.add_url_rule('/<int:transaction_id>', view_func=TransactionView.as_view('transaction'))
