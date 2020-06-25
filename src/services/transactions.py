from datetime import datetime
from decimal import (
    Decimal,
    ROUND_CEILING
)
from math import ceil

from exceptions import ServiceError
from flask import jsonify, url_for
from services.helper import update


class TransactionsServiceError(ServiceError):
    service = 'transactions'


class EmptyReportError(TransactionsServiceError):
    pass


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
        transaction_type = data.get('type', None)
        amount = data.get('amount', None)
        if type is not None:
            data['type'] = int(transaction_type)
        if amount is not None:
            amount = round(Decimal(amount), 2)
            if amount < 0:
                raise NegativeValue
            data['amount'] = amount
        return data

    def _parse_response(self, data):
        """
        Парсер специфичных полей выборки от БД для
        формирования корректных ответов.
        :param data: данные из БД
        :return data: преобразованные данные для ответа
        """
        transaction_type = data.get('type', None)
        amount = data.get('amount', None)
        if type is not None:
            data['type'] = bool(transaction_type)
        if amount is not None:
            data['amount'] = str(amount)
        return data

    def _get_categories(self, user_id, category_id, topdown=True):
        """
        Метод получения дерева категорий

        :param user_id: id авторизарованного пользователя
        :param category_id: id категории по которой проводится выборка
        :param topdown: параметр, определяющий путь обхода дерева. При True происходит обход от category_id
                        до конца дерева (т.е. вниз).
                        При False происходи обход дерева от category_id до корня дерева (т.е. вверх)
        :return: возвращает список словарей с id категорий в порядке обхода дерева.
        """

        if category_id is None and topdown:
            cursor = self.connection.execute('SELECT id, name FROM category WHERE user_id = ?', (user_id,))
            cursor = cursor.fetchall()
            return [dict(elem) for elem in cursor]
        elif category_id is None and not topdown:
            raise ValueError  # обход дерева вверх не зная начальной точки - "суперлогичная задача"

        if topdown:
            bypass_rule = 'WHERE c.parent_id = sc.id'
            what_to_select = 'id'
        else:
            bypass_rule = 'WHERE c.id = sc.parent_id'
            what_to_select = 'id, name'

        cursor = self.connection.execute(
            f'''
            WITH RECURSIVE sub_category(id, name, parent_id) AS (
                SELECT id, name, parent_id FROM category WHERE user_id = ? AND id = ?
                UNION ALL
                SELECT c.id, c.name, c.parent_id FROM category c, sub_category sc
                {bypass_rule}
            )
            SELECT {what_to_select} FROM sub_category;
            ''',
            (user_id, category_id)
        )
        cursor = cursor.fetchall()
        return [dict(elem) for elem in cursor]

    def _get_links(self, filters, total_items):
        """
        Метод формирования ссылок на следующую и предыдущую страницу пагинации
        с сохранением пользовательских фильтров

        :param filters: dict изначальных query params
        :param total_items: число всех полученных операций, исходя из фильтров
        :return: dict содержащий 2 поля - next_link и prev_link. Одно из них может быть пустой строкой.
        """

        # не отрицаю что все эти вычисления надо вынести в основную функцию, но пока функции нет, будут здесь
        page_size = int(filters.get('page_size'))
        current_page = int(filters.get('page'))
        if current_page is None:
            current_page = 1
        if page_size is None:
            page_size = 20

        pages = ceil(total_items / page_size)

        links = {}

        if current_page + 1 >= pages:
            links['next_link'] = ''
        else:
            next_page = current_page + 1

        if current_page - 1 <= 0:
            links['prev_link'] = ''
        else:
            prev_page = current_page - 1

        # копирование словаря с query params, просто потому что это имутабл дикт(кто это придумал, что за дебил)
        filters_to_add = {}
        for key, value in filters.items():
            filters_to_add[key] = value

        # формирование ссылок
        if 'next_link' not in links:
            filters_to_add['page'] = next_page
            links['next_link'] = url_for('transactions.transactions', **filters_to_add, _external=True)
        if 'prev_link' not in links:
            filters_to_add['page'] = prev_page
            links['prev_link'] = url_for('transactions.transactions', **filters_to_add, _external=True)

        return links

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
            (type_transaction, str(amount), description, date, user_id, category_id),
        )

        transaction_id = cursor.lastrowid
        if transaction_id is None:
            raise TransactionAddingFailedError()
        else:
            new_transaction['id'] = transaction_id
            new_transaction.pop('user_id')
            new_transaction['amount'] = str(amount)
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

    def _get_transactions(self, user_id, categories=[{'id': 1}]):   # TODO: убрать отладочную заглушку
        """
        Метод для получения сортированного списка операций по списку категорий,
        подсчёта суммы отчёта и количества элементов отчёта.

        :param user_id: парметры авторизации
        :param categories: список идентификаторов категорий
        :return: частично сформированный ответ
        """
        # Формируем условие
        clause = 'OR '.join(f'category_id = {category["id"]}' for category in categories)

        # Получаем сортрованный по дате список операций
        cursor = self.connection.execute(f'''
            SELECT id, date, type, description, amount, category_id
            FROM operation
            WHERE {clause}
            ORDER BY date ASC
        ''')
        transactions = cursor.fetchall()

        # Проверка на "пустой отчёт"
        if transactions is None:
            raise EmptyReportError
        transactions = [dict(transaction) for transaction in transactions]

        # Инициализация аккумулятора суммы, получение кол-ва элементов
        total = 0
        total_items = len(transactions)

        for transaction in transactions:
            # Формирование пути по категориям для операций
            category_id = transaction.pop('category_id')
            category_path = self._get_categories(user_id, category_id, topdown=False)
            transaction['categories'] = category_path

            # Подсчёт суммы по всему отчёту
            amount = Decimal(transaction['amount'])
            if bool(transaction['type']):
                total += amount
            else:
                total -= amount

        report = {
            'operations': transactions,
            'total': str(total),
            'total_items': total_items
        }

        return report

    def get_transaction(self, transaction_filters, user_id):

        #  это только заготовка редачить по своему усмотрению

        category_id = transaction_filters.get('category_id')

        filtered_categories = self._get_categories(user_id, category_id)
        print(filtered_categories)

        i_hate_links = self._get_links(transaction_filters, 100)

        return []
