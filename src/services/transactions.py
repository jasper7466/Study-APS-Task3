import calendar
from datetime import datetime, timedelta, date#, time as dt_time
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

  
class PageReportNotExist(TransactionsServiceError):
    pass


class TransactionInvalidPeriodError(TransactionsServiceError):
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

    def _is_owner_category(self, category_id, user_id):
        """
        Метод для проверки принадлежности операции пользователю.
        :param user_id: идентификатор пользователя
        :param category_id: идентификатор операции
        :return is_owner: True/False
        """
        cursor = self.connection.execute(
            """
            SELECT *
            FROM category
            WHERE id = ? AND user_id = ?
            """,
            (category_id, user_id,),
        )
        cursor = cursor.fetchone()
        if cursor is None:
            return False
        else:
            return True

    def _category_exist(self, category_id):
        """
        Метод для проверки на существование категории

        :param category_id: идентификатор проверяемой категории
        :return : bool переменную True - категория существует; False - категория не существует
        """
        cursor = self.connection.execute(
            """
            SELECT *
            FROM category
            WHERE id = ?
            """,
            (category_id,),
        )
        cursor = cursor.fetchone()
        if cursor is None:
            return False
        else:
            return True

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

        # Проверка на существование категории, если она указана
        if category_id:
            category_exist = self._category_exist(category_id)
            if not category_exist:
                raise CategoryNotExists()

        if category_id and user_id:
            owner = self._is_owner_category(category_id, user_id)
            if not owner:
                raise OtherUserCategory()

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
            (user_id, category_id,),
        )
        cursor = cursor.fetchall()
        return [dict(elem) for elem in cursor]

    def _get_links(self, filters, current_page, pages):
        """
        Метод формирования ссылок на следующую и предыдущую страницу пагинации
        с сохранением пользовательских фильтров

        :param filters: dict изначальных query params
        :param current_page: страница, которая отображается в данный момент времени
        :param pages: количество страниц в отчет
        :return: dict содержащий 2 поля - next_link и prev_link. Одно из них может быть пустой строкой.
        """

        links = {}

        if current_page == pages:
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

        return ''  # TODO странный return

    def _get_transactions(self, user_id, categories, page_size, offset_param, from_date, to_date, missing_category):
        """
        Метод для получения сортированного списка операций по списку категорий,
        подсчёта суммы отчёта и количества элементов отчёта.

        :param user_id: парметры авторизации
        :param categories: список идентификаторов категорий
        :param page_size: количество отобразаемых записей на странице
        :param offset_param: параметр для сдвига в выборке операций
        :param from_date: параметр указывающий с какой даты делать выборку
        :param to_date: параметр указывающий по какую дату делать выборку
        :param missing_category: параметр указывающий необходимо ли включать в выборку безкатегорийные операции
        :return: частично сформированный ответ
        """
        # Формируем условие
        clause = ' OR '.join(f'category_id = {category["id"]}' for category in categories)
        if missing_category:
            clause = f'({clause} OR (category_id IS NULL))'
        if from_date:
            clause = clause + f' AND (date>={from_date})'
        if to_date:
            clause = clause + f' AND (date<{to_date})'

        # формируем основное тело запроса
        sql_request = f'''
            SELECT id, date, type, description, amount, category_id
            FROM operation
            WHERE {clause} AND user_id = {user_id}
            ORDER BY date ASC
        '''
        cursor = self.connection.execute(sql_request)
        transactions = cursor.fetchall()

        # Проверка на "пустой отчёт"
        if transactions is None:
            raise EmptyReportError

        # Инициализация аккумулятора суммы, получение кол-ва элементов
        total = 0
        total_items = len(transactions)
        # Подсчёт суммы по всему отчёту
        for transaction in transactions:
            amount = Decimal(transaction['amount'])
            if bool(transaction['type']):
                total += amount
            else:
                total -= amount

        # Добавление в зарос параметров LIMIT и OFFSET для пагинации
        sql_request = sql_request + f'LIMIT {page_size} OFFSET {offset_param}'
        cursor = self.connection.execute(sql_request)
        transactions = cursor.fetchall()
        transactions = [dict(transaction) for transaction in transactions]

        for transaction in transactions:
            # Формирование пути по категориям для операций
            if transaction['category_id'] is not None:
                category_id = transaction.pop('category_id')
                category_path = self._get_categories(user_id, category_id, topdown=False)
                transaction['categories'] = category_path
            else:
                transaction.pop('category_id')
                transaction['categories'] = []

        report = {
            'operations': transactions,
            'total': str(total),
            'total_items': total_items
        }
        return report

    def _week(self, reference=datetime.today()):
        """
        Утилита для получения границ текщей недели.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        result['from'] = reference + timedelta(0 - reference.weekday())
        result['to'] = reference + timedelta(7 - reference.weekday())
        return result

    def _last_week(self, reference=datetime.today()):
        """
        Утилита для получения границ прошлой недели.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        result['from'] = reference + timedelta(0 - reference.weekday() - 7)
        result['to'] = reference + timedelta(7 - reference.weekday() - 7)
        return result

    def _month(self, reference=datetime.today()):
        """
        Утилита для получения границ текщего месяца.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()

        if reference.month == 12:   # Смена года для границы 'to'
            month = 1
            year = reference.year + 1
        else:
            month = reference.month + 1
            year = reference.year

        result['from'] = reference.replace(day=1)
        result['to'] = reference.replace(day=1, month=month, year=year)
        return result

    def _last_month(self, reference=datetime.today()):
        """
        Утилита для получения границ прошлого месяца.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()

        if reference.month == 1:    # Смена года для границы 'from'
            month = 12
            year = reference.year - 1
        else:
            month = reference.month - 1
            year = reference.year

        result['from'] = reference.replace(day=1, month=month, year=year)
        result['to'] = reference.replace(day=1)
        return result

    def _quarter(self, reference=datetime.today()):
        """
        Утилита для получения границ текщего квартала.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()

        quarter = (reference.month - 1) // 3
        month_from = quarter * 3 + 1
        month_to = month_from + 3
        year = reference.year

        if quarter == 3:        # Смена года для границы 'to'
            month_to = 1
            year = reference.year + 1

        result['from'] = reference.replace(day=1, month=month_from)
        result['to'] = reference.replace(day=1, month=month_to, year=year)

        return result

    def _last_quarter(self, reference=datetime.today()):
        """
        Утилита для получения границ прошлого квартала.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()

        quarter = (reference.month - 1) // 3
        year = reference.year

        if quarter == 0:        # Смена года для границы 'from'
            quarter = 3
            year = reference.year - 1
        else:
            quarter = quarter - 1

        month_from = quarter * 3 + 1
        month_to = month_from + 3

        if quarter == 3:        # Смена месяца для границы 'to'
            month_to = 1

        result['from'] = reference.replace(day=1, month=month_from, year=year)
        result['to'] = reference.replace(day=1, month=month_to)

        return result

    def _year(self, reference=datetime.today()):
        """
        Утилита для получения границ текщего года.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        year = reference.year + 1
        result['from'] = reference.replace(day=1, month=1)
        result['to'] = reference.replace(day=1, month=1, year=year)
        return result

    def _last_year(self, reference=datetime.today()):
        """
        Утилита для получения границ прошлого года.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        year = reference.year - 1
        result['from'] = reference.replace(day=1, month=1, year=year)
        result['to'] = reference.replace(day=1, month=1)
        return result

    def _get_period(self, period):
        """
        Утилита для формирования границ временного интервала по заданному типу.

        :param period: тип интервала (week, last_week, month, last_month, quarter, last_quarter, year, last_year)
        :return: {'from': date_from, 'to': date_to} (стандарт UTC, формат timestamp)
        """
        if period == 'week':                    # Период - текущая неделя
            result = self._week()
        elif period == 'last_week':             # Период - прошлая неделя
            result = self._last_week()
        elif period == 'month':                 # Период - текущий месяц
            result = self._month()
        elif period == 'last_month':            # Период - прошлый месяц
            result = self._last_month()
        elif period == 'quarter':               # Период - текущий квартал
            result = self._quarter()
        elif period == 'last_quarter':          # Период - прошлый квартал
            result = self._last_quarter()
        elif period == 'year':                  # Период - текущий год
            result = self._year()
        elif period == 'last_year':             # Период - прошлый год
            result = self._last_year()
        else:
            raise TransactionInvalidPeriodError

        # Сброс времени и преобразование в UTC timestamp
        for key in result:
            result[key] = result[key].replace(hour=0, minute=0, second=0, microsecond=0)
            result[key] = calendar.timegm(result[key].utctimetuple())

        return result

    def get_transaction(self, transaction_filters, user_id):
        """
        Метод для получения полного отчета, при заданных пользовательских условиях.

        :param transaction_filters: словар, включаущий в себя query-параметры
        :param user_id: идентификатор авторизованного пользователя
        :return:        Полный отчет включает в себя:
                        Список операций, удовлетворяющих пользовательским условиям;
                        Сумму по всему отчёту;
                        Количество элементов в отчёте;
                        Количество страниц в отчёте;
                        Максимальное количество элементов на странице;
                        Текущую страницу отчёта;
                        Ссылку на получение следующей страницы отчёта;
                        Ссылку на получение предыдущей страницы отчёта.
        """
        category_id = transaction_filters.get('category_id')
        from_date = transaction_filters.get('from')
        to_date = transaction_filters.get('to')
        period = transaction_filters.get('period', None)
        page_size = transaction_filters.get('page_size')
        current_page = transaction_filters.get('page')

        # Проверка важных входных объектов, при их отсутствии устанавливаются значения по умолчанию
        if category_id is None:
            missing_category = True
        else:
            category_id = int(category_id)
            missing_category = False

        if current_page is None:
            current_page = 1
        else:
            current_page = int(current_page)

        if page_size is None:
            page_size = 20
        else:
            page_size = int(page_size)

        if from_date:
            from_date = int(from_date)

        if to_date:
            to_date = int(to_date)
        offset_param = (current_page-1) * page_size

        if period is not None:
            range = self._get_period(period)
            from_date = range['from']
            to_date = range['to']

        filtered_categories = self._get_categories(user_id, category_id)
        report = self._get_transactions(user_id, filtered_categories, page_size, offset_param, from_date, to_date,
                                        missing_category)
        pages = ceil(report['total_items'] / page_size)

        if current_page > pages:
            raise PageReportNotExist

        links = self._get_links(transaction_filters, current_page, pages)

        if links['prev_link']:
            report['prev_page'] = links['prev_link']
        if links['next_link']:
            report['next_page'] = links['next_link']

        report['total_pages'] = pages
        report['page'] = current_page
        report['page_size'] = page_size
        return report
