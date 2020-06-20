from database import db
from flask import (
    Blueprint,
    request
)
from flask.views import MethodView
from services.transactions import (
    TransactionsService,
    MissingImportantFields,
    NegativeValue,
    CategoryNotExists,
    OtherUserCategory,
    TransactionAddingFailedError,
    OtherUserTransaction,
    TransactionNotExists
)
from services.decorators import auth_required

bp = Blueprint('transactions', __name__)


class TransactionsView(MethodView):
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


class TransactionDeleteView(MethodView):
    @auth_required
    def delete(self, user, transaction_id):
        """
        Обработчик DELETE-запроса на удаление существующей операции
        :param user: идентификатор авторизованного пользователя
        :param transaction_id: идентификатор удаляемой операции
        :return:
        """
        data_to_delete = {
            'user_id': user['id'],
            'transaction_id': transaction_id
        }

        with db.connection as connection:
            service = TransactionsService(connection)

            try:
                deleted_transaction = service.delete_transaction(data_to_delete)
            except OtherUserTransaction:
                return '', 403
            except TransactionNotExists:
                return '', 404
            else:
                return deleted_transaction, 200


bp.add_url_rule('', view_func=TransactionsView.as_view('transactions'))
bp.add_url_rule('/<int:transaction_id>', view_func=TransactionDeleteView.as_view('transaction'))
