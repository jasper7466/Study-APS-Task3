import sqlite3 as sqlite

from exceptions import ServiceError
from flask import jsonify
from services.helper import (
    insert,
    update
)


class CategoryServiceError(ServiceError):
    service = 'categories'


class CategoryDoesNotExistError(CategoryServiceError):
    pass


class CategoryPatchError(CategoryServiceError):
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

    def patch(self, data, category_id, user_id):
        """
        Метод, реализующий бизнес-логику эндпоинта редактирования категории.

        :param data: новые параметры категории
        :param category_id: идентификатор категории
        :param user_id: идентификатор пользователя
        :return patched: параметры отредактированной категории
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

    def add_category(self, data):
        """
        Метод, реализующий бизнес-логику эндпоинта создания категории.

        :param data: параметры создаваемой категории
        :return created: параметры созданной категории
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
        Метод-сервис по удалению категорий, осуществляющий основную работу с БД.

        :param category: dict, содержащий поля, переданные в запросе + id пользователя
        :return:
        """
        user_id = category.get('user_id')
        category_id = category.get('category_id')

        # Проверка на существование категории
        existed_category = self.connection.execute(
            """
            SELECT * FROM category
            WHERE id = ?
            """,
            (category_id,)
        )
        existed_category = existed_category.fetchone()
        if not existed_category:
            raise CategoryDoesNotExistError()

        # Проверка на принадлежность категории пользователю
        existed_category = self.connection.execute(
            """
            SELECT * FROM category
            WHERE user_id = ? AND id = ?
            """,
            (user_id, category_id)
        )
        existed_category = existed_category.fetchone()
        if not existed_category:
            raise CategoryAccessDeniedError(category)

        # Переделываем все связанные операции в безкатегорийные
        self.connection.execute(
            '''
            UPDATE operation
            SET category_id = NULL
            WHERE user_id = ? AND category_id = ?
            ''',
            (user_id, category_id)
        )
        self.connection.commit()

        # Делаем дочерние категории родительскими
        self.connection.execute(
            '''
            UPDATE category
            SET parent_id = NULL
            WHERE parent_id = ?
            ''',
            (category_id,)
        )
        self.connection.commit()

        # Удаляем категорию
        self.connection.execute(
            '''
            DELETE FROM category
            WHERE id = ? 
            ''',
            (category_id,)
        )
        self.connection.commit()

        return '', 200  # TODO вынести работу с кодами в bp

    def get_category(self, category):
        """
        Метод-сервис по получению категорий, осуществляющий основную работу с БД

        :param category: dict, содержащий поля, переданные в запросе + id пользователя
        :return:
        """
        user_id = category.get('user_id')
        category_name = category.get('name')

        # Проверка на существование категории
        existed_category = self.connection.execute(
            """
            SELECT * FROM category
            WHERE name = ? AND user_id = ?
            """,
            (category_name, user_id)
        )
        existed_category = existed_category.fetchone()
        if not existed_category:
            raise CategoryDoesNotExistError()

        existed_category = dict(existed_category)
        existed_category.pop('user_id')
        if existed_category.get('parent_id') is None:
            existed_category.pop('parent_id')
        return dict(existed_category)   # TODO вынести работу с кодами в bp

    def _get_category(self, category):
        """
        Метод для получения параметров категории по её имени и
        идентификатору пользователя.

        :param category: параметрами запроса
        :return: параметры запрашиваемой категории
        """
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
