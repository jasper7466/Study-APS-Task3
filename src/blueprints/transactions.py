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
    TransactionAddingFailedError
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


bp.add_url_rule('', view_func=TransactionsView.as_view('transactions'))
