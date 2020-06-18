from exceptions import ServiceError
from flask import jsonify
from decimal import Decimal, ROUND_CEILING
from datetime import datetime


class TransactionServiceError(ServiceError):
    service = 'transactions'


class MissingImportantFields(TransactionServiceError):
    pass


class NegativeValue(TransactionServiceError):
    pass


class CategoryNotExists(TransactionServiceError):
    pass


class OtherUserCategory(TransactionServiceError):
    pass


class TransactionAddingFailedError(TransactionServiceError):
    pass


class TransactionsService:
    def __init__(self, connection):
        self.connection = connection

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
