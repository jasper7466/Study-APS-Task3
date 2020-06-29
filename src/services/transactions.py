import calendar
from datetime import datetime, timedelta, date
from decimal import (
    Decimal,
    ROUND_CEILING
)
from math import ceil
from exceptions import ServiceError
from flask import url_for
from services.helper import (
    insert,
    update,
    delete
)


class TransactionsServiceError(ServiceError):
    service = 'transactions'


class EmptyReportError(TransactionsServiceError):
    pass


class TransactionDoesNotExistError(TransactionsServiceError):
    pass


class TransactionAccessDeniedError(TransactionsServiceError):
    pass


class DataBaseConflictError(TransactionsServiceError):
    pass


class MissingRequiredFields(TransactionsServiceError):
    pass


class NegativeValue(TransactionsServiceError):
    pass


class CategoryDoesNotExistError(TransactionsServiceError):
    pass


class CategoryAccessDeniedError(TransactionsServiceError):
    pass

  
class PageReportNotExist(TransactionsServiceError):
    pass


class TransactionInvalidPeriodError(TransactionsServiceError):
    pass


class TransactionsService:
    def __init__(self, connection):
        self.connection = connection

    def add_transaction(self, data):
        """
        Метод, реализующий бизнес-логику эндпоинта создания новой операции.

        :param data: параметры создаваемой операции
        :return: параметры созданной операции
        """
        # Получение/преобразование специфичных данных для дальнейшей работы с ними
        data = self._parse_request(data)

        # Проверка на наличие обязательных полей
        if data['type'] is None or data['amount'] is None:
            raise MissingRequiredFields()

        # Проверка на существование категории и её принадлежность пользователю (если указана)
        if data['category_id']:
            self._is_owner_category(data['category_id'], data['user_id'])

        # Вставка в таблицу БД
        instance_id = insert('operation', data, self.connection)
        if instance_id is None:
            raise DataBaseConflictError(data)

        # Запрос на получение созданной операции и возврат преобразованных для ответа данных
        created = self._get_transaction(instance_id)
        return self._parse_response(created)

    def get_transaction(self, transaction_filters, user_id):
        """
        Метод, реализующий бизнес-логику эндпоинта получения полного отчета
        при заданных пользовательских условиях.

        :param transaction_filters: словарь, включаущий в себя query-параметры
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
        category_id = transaction_filters.get('category_id', None)
        from_date = transaction_filters.get('from', None)
        to_date = transaction_filters.get('to', None)
        period = transaction_filters.get('period', None)
        page_size = transaction_filters.get('page_size', None)
        current_page = transaction_filters.get('page', None)

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

    def patch_transaction(self, transaction_id, user_id, data):
        """
        Метод, реализующий бизнес-логику эндпоинта редактирования существующей операции.

        :param transaction_id: идентификатор операции
        :param user_id: идентификатор пользователя
        :param data: обновляемые данные
        :return: сформированный ответ
        """
        # Проверка на существование операции и её принадлежность пользователю
        self._is_owner_transaction(transaction_id, user_id)

        # Преобразование специфичных полей данных
        data = self._parse_request(data)

        # Проверка на существование категории и её принадлежность пользователю (если указана)
        if data['category_id'] is not None:
            self._is_owner_category(data['category_id'], user_id)

        is_patched = update('operation', data, transaction_id, self.connection)
        if not is_patched:
            raise DataBaseConflictError
        else:
            patched = self._get_transaction(transaction_id)
            return self._parse_response(patched)

    def delete_transaction(self, data):
        """
        Метод, реализующий бизнес-логику эндпоинта удаления существующей операции.

        :param data: параметры операции
        :return: nothing
        """
        user_id = data.get('user_id')
        transaction_id = data.get('transaction_id')

        # Проверка на существование операции и её принадлежность пользователю
        self._is_owner_transaction(transaction_id, user_id)

        # Удаляем операцию
        is_deleted = delete('operation', transaction_id, self.connection)
        if not is_deleted:
            raise DataBaseConflictError

    def _get_transaction(self, transaction_id):
        """
        Метод для получения параметров операции по её идентификатору.

        :param transaction_id: идентификатор операции
        :return: параметры операции
        """
        cur = self.connection.execute(f'SELECT * FROM operation WHERE id = "{transaction_id}"')
        transaction = cur.fetchone()
        if not transaction:
            raise TransactionDoesNotExistError(transaction_id)
        else:
            return dict(transaction)

    def _is_owner_transaction(self, transaction_id, user_id):
        """
        Метод для проверки принадлежности операции пользователю.

        :param user_id: идентификатор пользователя
        :param transaction_id: идентификатор операции
        :return: True or raise exception
        """
        transaction = self._get_transaction(transaction_id)
        if transaction['user_id'] != user_id:
            raise TransactionAccessDeniedError
        return True

    def _is_owner_category(self, category_id, user_id):
        """
        Метод для проверки принадлежности категории пользователю.

        :param user_id: идентификатор пользователя
        :param category_id: идентификатор категории
        :return: True or raise exception
        """
        cursor = self.connection.execute(f'SELECT * FROM category WHERE id = {category_id}')
        instance = cursor.fetchone()
        if instance is None:
            raise CategoryDoesNotExistError(category_id)
        if instance['user_id'] != user_id:
            raise CategoryAccessDeniedError
        return True

    def _category_exist(self, category_id):
        """
        Метод для проверки существования категории.

        :param category_id: идентификатор категории
        :return: True/False
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

    @staticmethod
    def _parse_request(data):
        """
        Парсер специфичных полей запроса из JSON-формата в Python-формат для дальнейшей
        корректной работы с ними.

        :param data: данные запроса
        :return: преобразованные данные запроса
        """
        data['type'] = data.get('type', None)
        data['amount'] = data.get('amount', None)
        data['category_id'] = data.get('category_id', None)
        now = calendar.timegm(datetime.now().utctimetuple())
        data['date'] = data.get('date', int(now))
        data['description'] = data.get('description', None)

        if data['type'] is not None:
            data['type'] = int(data['type'])

        if data['amount'] is not None:
            data['amount'] = round(Decimal(data['amount']), 2)
            if data['amount'] < 0:
                raise NegativeValue
        return data

    @staticmethod
    def _parse_response(data):
        """
        Парсер специфичных полей выборки от БД для формирования корректных JSON-ответов.

        :param data: данные из БД
        :return: преобразованные данные для ответа
        """
        transaction_type = data.get('type', None)
        amount = data.get('amount', None)
        if transaction_type is not None:
            data['type'] = bool(transaction_type)
        if amount is not None:
            data['amount'] = str(amount)
        return data

    def _get_categories(self, user_id, category_id, top_down=True):
        """
        Метод получения дерева категорий

        :param user_id: идентификатор пользователя
        :param category_id: идентификатор категории, по которой проводится выборка
        :param top_down: параметр, определяющий путь обхода дерева. При True происходит обход от category_id
                        до конца дерева (т.е. вниз).
                        При False происходит обход дерева от category_id до корня дерева (т.е. вверх)
        :return: возвращает список словарей с параметрами категорий в порядке обхода дерева.
        """

        # Проверка на существование категории, если она указана
        if category_id:
            category_exist = self._category_exist(category_id)
            if not category_exist:
                raise CategoryDoesNotExistError()

        if category_id and user_id:
            self._is_owner_category(category_id, user_id)

        if category_id is None and top_down:
            cursor = self.connection.execute('SELECT id, name FROM category WHERE user_id = ?', (user_id,))
            cursor = cursor.fetchall()
            return [dict(elem) for elem in cursor]
        elif category_id is None and not top_down:
            raise ValueError  # обход дерева вверх не зная начальной точки

        if top_down:
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

    @staticmethod
    def _get_links(filters, current_page, pages):
        """
        Метод формирования ссылок на следующую и предыдущую страницу пагинации
        с сохранением пользовательских фильтров.

        :param filters: dict изначальных query params
        :param current_page: страница, которая отображается в данный момент времени
        :param pages: количество страниц в отчет
        :return: dict содержащий 2 поля - next_link и prev_link. Одно из них может быть пустой строкой.
        """

        links = {}
        next_page = 1
        prev_page = 1

        if current_page == pages:
            links['next_link'] = ''
        else:
            next_page = current_page + 1

        if current_page - 1 <= 0:
            links['prev_link'] = ''
        else:
            prev_page = current_page - 1

        # копирование словаря с query params, просто потому что это имутабл дикт
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
                category_path = self._get_categories(user_id, category_id, top_down=False)
                transaction['categories'] = category_path
            else:
                transaction.pop('category_id')
                transaction['categories'] = []
            # Преобразование специфичных полей операции
            transaction = self._parse_response(transaction)

        report = {
            'operations': transactions,
            'total': str(total),
            'total_items': total_items
        }
        return report

    @staticmethod
    def _week(reference=datetime.today()):
        """
        Утилита для получения границ текущей недели.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        result['from'] = reference + timedelta(0 - reference.weekday())
        result['to'] = reference + timedelta(7 - reference.weekday())
        return result

    @staticmethod
    def _month(reference=datetime.today()):
        """
        Утилита для получения границ текущего месяца.

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

    @staticmethod
    def _quarter(reference=datetime.today()):
        """
        Утилита для получения границ текущего квартала.

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

    @staticmethod
    def _last_quarter(reference=datetime.today()):
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

    @staticmethod
    def _year(reference=datetime.today()):
        """
        Утилита для получения границ текущего года.

        :param reference: опорная дата (по умолчанию - сегодня)
        :return: {'from': date_from, 'to': date_to} (в стандарте GMT)
        """
        result = dict()
        year = reference.year + 1
        result['from'] = reference.replace(day=1, month=1)
        result['to'] = reference.replace(day=1, month=1, year=year)
        return result

    def _get_period(self, period):
        """
        Утилита для формирования границ временного интервала по заданному типу периода.

        :param period: тип интервала (week, last_week, month, last_month, quarter, last_quarter, year, last_year)
        :return: {'from': date_from, 'to': date_to} (стандарт UTC, формат timestamp)
        """
        if period == 'week':                    # Период - текущая неделя
            result = self._week()
        elif period == 'last_week':             # Период - прошлая неделя
            shift = datetime.today() + timedelta(-7)
            result = self._week(shift)
        elif period == 'month':                 # Период - текущий месяц
            result = self._month()
        elif period == 'last_month':            # Период - прошлый месяц
            now = datetime.today()
            month_size = calendar.monthrange(now.year, now.month)[1]
            shift = now + timedelta(days=-month_size)
            result = self._month(shift)
        elif period == 'quarter':               # Период - текущий квартал
            result = self._quarter()
        elif period == 'last_quarter':          # Период - прошлый квартал
            result = self._last_quarter()
        elif period == 'year':                  # Период - текущий год
            result = self._year()
        elif period == 'last_year':             # Период - прошлый год
            now = datetime.today()
            shift = now.replace(day=1, year=now.year-1)
            result = self._year(shift)
        else:
            raise TransactionInvalidPeriodError

        # Сброс времени и преобразование в UTC timestamp
        for key in result:
            result[key] = result[key].replace(hour=0, minute=0, second=0, microsecond=0)
            result[key] = calendar.timegm(result[key].utctimetuple())

        return result
