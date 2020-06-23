import sqlite3 as sqlite
from exceptions import ServiceError
from services.helper import (
    insert,
    update,
    delete
)


class CategoryServiceError(ServiceError):
    service = 'categories'


class CategoryDoesNotExistError(CategoryServiceError):
    pass


class CategoryPatchError(CategoryServiceError):
    pass


class CategoryDeleteError(CategoryServiceError):
    pass


class CategoryCreateError(CategoryServiceError):
    pass


class CategoryFullCopyError(CategoryServiceError):
    pass


class CategoryAccessDeniedError(CategoryServiceError):
    pass


class CategoriesService:
    def __init__(self, connection):
        self.connection = connection

    def patch_category(self, data, category_id, user_id):
        """
        Метод, реализующий бизнес-логику эндпоинта редактирования категории.

        :param data: новые параметры категории
        :param category_id: идентификатор категории
        :param user_id: идентификатор пользователя
        :return: параметры отредактированной категории
        """
        # Проверка на существование категории и её принадлежность пользователю
        self._is_owner(category_id, user_id)

        # Получение полей запроса
        name = data.get('name', None)
        parent_id = data.get('parent_id', -1)

        # Проверка наличия параметра "parent_id" в запросе
        if parent_id is not None:
            if parent_id > 0:
                # Проверка на существование родительской категории и её принадлежность пользователю
                self._is_owner(parent_id, user_id)
        else:
            # В случае запроса на преобразование в категорию верхнего уровня (передано null)
            data['parent_id'] = None

        # Проверка наличия/дублирования категории с параметром "name" в дереве пользователя
        if name is not None:
            duplicate = self._duplicated_name(name, user_id, category_id)
            if duplicate is not None:
                raise CategoryPatchError

        # Обновление параметров категории
        is_patched = update('category', data, category_id, self.connection)
        if not is_patched:
            raise CategoryPatchError
        else:
            patched = self._get_category_by_id(category_id)
            patched.pop('user_id')
            return patched

    def create_category(self, data):
        """
        Метод, реализующий бизнес-логику эндпоинта создания категории.

        :param data: параметры создаваемой категории
        :return: параметры созданной категории
        """
        # Получение полей запроса
        name = data.get('name', None)
        user_id = data.get('user_id', None)
        parent_id = data.get('parent_id', None)

        # Проверка на существование и принадлежность родительской категории пользователю
        if parent_id is not None:
            self._is_owner(parent_id, user_id)

        # Поиск дублирующихся категорий по имени
        category = self._duplicated_name(name, user_id)

        # Проверка на полную копию
        if category is not None:
            if category['parent_id'] == parent_id:
                raise CategoryFullCopyError     # Полная копия
            else:
                raise CategoryCreateError       # Конфликт полей parent_id

        # Запись в БД
        category_id = insert('category', data, self.connection)
        if category_id is None:
            raise CategoryCreateError

        # Получение записанной категории из БД
        created = self._get_category_by_id(category_id)

        # Формирование требуемого ответа
        created.pop('user_id')
        if created.get('parent_id') is None:
            created.pop('parent_id')
        return dict(created)

    def delete_category(self, category):
        """
        Метод, реализующий бизнес-логику эндпоинта удаления категории.

        :param category: параметры удаляемой категории
        :return: nothing
        """
        # Получение полей запроса
        user_id = category.get('user_id')
        category_id = category.get('category_id')

        # Проверка на существование и принадлежность категории пользователю
        self._is_owner(category_id, user_id)

        # Переделываем все связанные операции в безкатегорийные
        data = {'category_id': None}
        success = update('operation', data, category_id, self.connection, 'category_id')
        if not success:
            raise CategoryDeleteError

        # Делаем дочерние категории родительскими
        data = {'parent_id': None}
        success = update('category', data, category_id, self.connection, 'parent_id')
        if not success:
            raise CategoryDeleteError

        # Удаляем категорию
        success = delete('category', category_id, self.connection)
        if not success:
            raise CategoryDeleteError

    def get_category(self, data):
        """
        Метод, реализующий бизнес-логику эндпоинта получения категории по её имени.

        :param data: параметры запрашиваемой категории
        :return: параметры категории
        """
        # Получение категории и проверка на существование
        category = self._get_category(data)

        # Проверка на существование и принадлежность категории пользователю
        self._is_owner(category['id'], data['user_id'])

        category.pop('user_id')
        if category.get('parent_id') is None:
            category.pop('parent_id')
        return dict(category)

    def _get_category(self, category):
        """
        Метод для получения параметров категории по её имени и
        идентификатору пользователя.

        :param category: параметрами запроса
        :return: параметры запрашиваемой категории
        """
        # Получение полей запроса
        user_id = category.get('user_id')
        name = category.get('name')

        cur = self.connection.execute(f'SELECT * FROM category WHERE name = "{name}" AND user_id = "{user_id}"')
        category = cur.fetchone()
        if not category:
            raise CategoryDoesNotExistError(category)
        else:
            return dict(category)

    def _get_category_by_id(self, category_id):
        """
        Метод для получения параметров категории по её идентификатору.

        :param category_id: идентификатор категории
        :return: параметры запрашиваемой категории
        """
        cur = self.connection.execute(f'SELECT * FROM category WHERE id = "{category_id}"')
        category = cur.fetchone()
        if not category:
            raise CategoryDoesNotExistError(category_id)
        else:
            return dict(category)

    def _is_owner(self, category_id, user_id):
        """
        Метод для проверки принадлежности категории пользователю.

        :param user_id: идентификатор пользователя
        :param category_id: идентификатор категории
        :return: True or rise CategoryAccessDeniedError
        """
        category = self._get_category_by_id(category_id)
        if user_id != category['user_id']:
            raise CategoryAccessDeniedError
        else:
            return True

    def _duplicated_name(self, name, user_id, category_id=None):
        """
        Метод для проверки на наличие дубликата создаваемой/редактируемой
        категории с таким же именем (проверка на дубликат).

        :param name: имя категории
        :param user_id: идентификатор пользователя
        :return: параметры категории или None
        """
        data = dict()
        data['name'] = name
        data['user_id'] = user_id
        try:
            category = self._get_category(data)
        except CategoryDoesNotExistError:
            return None
        else:
            if category['id'] == category_id:   # Если найденный дубликат является редактируемой категорией
                return None
            else:
                return category
