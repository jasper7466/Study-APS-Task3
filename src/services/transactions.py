import sqlite3 as sqlite
from flask import jsonify
from datetime import datetime
from decimal import (
    Decimal,
    ROUND_CEILING
)
from exceptions import ServiceError
from services.helper import update


class TransactionsServiceError(ServiceError):
    service = 'transactions'


class TransactionDoesNotExistError(TransactionsServiceError):
    pass


class TransactionAccessDeniedError(TransactionsServiceError):
    pass


class TransactionPatchError(TransactionsServiceError):
    pass


class MissingImportantFields(TransactionsServiceError):
    pass


class NegativeValue(TransactionsServiceError):
    pass


class CategoryNotExists(TransactionsServiceError):
    pass


class OtherUserCategory(TransactionsServiceError):
    pass


class TransactionAddingFailedError(TransactionsServiceError):
    pass


class TransactionNotExists(TransactionsServiceError):
    pass


class OtherUserTransaction(TransactionsServiceError):
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

    def _parse_request(self, data):
        """
        Парсер специфичных полей запроса для дальнейшей
        корректной работы с ними.
        :param data: данные запроса
        :return data: преобразованные данные запроса
        """
        type = data.get('type', None)
        amount = data.get('amount', None)
        if type is not None:
            data['type'] = int(type)
        if amount is not None:
            amount = round(Decimal(amount), 2)
            data['amount'] = amount
        return data

    def _parse_response(self, data):
        """
        Парсер специфичных полей выборки от БД для
        формирования корректных ответов.
        :param data: данные из БД
        :return data: преобразованные данные для ответа
        """
        type = data.get('type', None)
        amount = data.get('amount', None)
        if type is not None:
            data['type'] = bool(type)
        if amount is not None:
            data['amount'] = str(amount)
        return data

    def patch_transaction(self, transaction_id, user_id, data):
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

        data = self._parse_request(data)
        is_patched = update('operation', data, transaction_id, self.connection)
        if not is_patched:
            raise TransactionPatchError
        else:
            patched = self._get_transaction(transaction_id)
            return self._parse_response(patched)

    def add_transaction(self, new_transaction):
        """
        Метод для создания новой операции
        :param new_transaction: поля новой операции операции
        :return new_transaction: новая операция
        """
        type_transaction = new_transaction.get('type')
        amount = new_transaction.get('amount')
        date = new_transaction.get('date')
        description = new_transaction.get('description')
        user_id = new_transaction.get('user_id')
        category_id = new_transaction.get('category_id')

        # Проверка на наличие важных полей
        if type_transaction is None or not amount:
            raise MissingImportantFields()

        # Проверка на тип операции(True == 1; False == 0)
        if type_transaction:
            type_transaction = int(1)
        else:
            type_transaction = int(0)

        if date is None:
            date = int(datetime.now().timestamp())

        if Decimal(amount) < 0:
            raise NegativeValue()
        else:
            amount = Decimal(amount).quantize(Decimal("1.00"), ROUND_CEILING)

        # Проверка на существование категории, если она указана
        if category_id:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM category
                WHERE id = ?
                """,
                (category_id,),
            )
            cursor = cursor.fetchone()
            if not cursor:
                raise CategoryNotExists()

        # Проверка, что указанная категория не принадлежит другому пользователю
        if category_id:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM category
                WHERE id = ? and user_id = ?
                """,
                (category_id, user_id,),
            )
            cursor = cursor.fetchone()
            if not cursor:
                raise OtherUserCategory()

        cursor = self.connection.execute(
            """
            INSERT INTO operation (type, amount, description, date, user_id, category_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (type_transaction, float(amount), description, date, user_id, category_id),
        )

        transaction_id = cursor.lastrowid
        if transaction_id is None:
            raise TransactionAddingFailedError()
        else:
            new_transaction['id'] = transaction_id
            new_transaction.pop('user_id')
            new_transaction['amount'] = float(amount)
            new_transaction['date'] = date
        return jsonify(new_transaction)

    def delete_transaction(self, delete_transaction):
        """
        Метод для удаления существующей транзакции
        """
        user_id = delete_transaction.get('user_id')
        transaction_id = delete_transaction.get('transaction_id')

        # Проверка на существование операции
        cursor = self.connection.execute(
            """
            SELECT *
            FROM operation
            WHERE id = ?
            """,
            (transaction_id,),
        )
        cursor = cursor.fetchone()
        if not cursor:
            raise TransactionNotExists()

        # Проверка на принадлежность операции пользователю
        cursor = self.connection.execute(
            """
            SELECT *
            FROM operation
            WHERE id = ? AND user_id =?
            """,
            (transaction_id, user_id,),
        )
        cursor = cursor.fetchone()
        if not cursor:
            raise OtherUserTransaction()

        # Удаляем операцию
        self.connection.execute(
            """
            DELETE FROM operation
            WHERE id = ?
            """,
            (transaction_id,),
        )

        return ''   # TODO странный return
