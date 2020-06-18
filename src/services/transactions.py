import sqlite3 as sqlite
from services.helper import update


class TransactionsServiceError(ServiceError):
    service = 'transactions'


class TransactionDoesNotExistError(RegisterServiceError):
    pass


class TransactionAccessDeniedError(RegisterServiceError):
    pass


class TransactionsService:
    def __init__(self, connection):
        self.connection = connection

    def _get_transaction(self, transaction_id):
        """
        Метод для получения параметров операции по её идентификатору.
        :param transaction_id: идентификатор операции
        :return transaction: операция
        """
        cur = self.connection.execute(f'SELECT * FROM operation WHERE id = "{transaction_id}"')
        transaction = cur.fetchone()
        if not transaction:
            raise TransactionDoesNotExistError(transaction_id)
        else:
            return dict(transaction)

    def _is_owner(self, transaction_id, user_id):
        """
        Метод для проверки принадлежности операции пользователю.
        :param user_id: идентификатор пользователя
        :param transaction_id: идентификатор операции
        :return is_owner: True/False
        """
        transaction = self._get_transaction(transaction_id)
        owner_id = transaction['user_id']
        return user_id == owner_id

    def patch(self, transaction_id, user_id, data):
        """
        Метод для редактирования существующей операции.
        :param transaction_id: идентификатор операции
        :param user_id: идентификатор пользователя
        :param data: обновляемые данные
        :return response: сформированный ответ
        """
        owner = self._is_owner(transaction_id, user_id)
        if not owner:
            raise TransactionAccessDeniedError
        update('operation', data, transaction_id, self.connection)




