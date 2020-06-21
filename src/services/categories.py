import sqlite3 as sqlite

from exceptions import ServiceError
from flask import jsonify
from services.helper import update


class CategoryServiceError(ServiceError):
    service = 'categories'


class CategoryDoesNotExistError(CategoryServiceError):
    pass


class CategoryPatchError(CategoryServiceError):
    pass


class OtherUserCategory(CategoryServiceError):
    pass


class CategoryAddingFailedError(CategoryServiceError):
    pass


class CategoryAccessDeniedError(CategoryServiceError):
    pass


class CategoriesService:
    def __init__(self, connection):
        self.connection = connection

    def add_category(self, new_category):
        """
        Метод-сервис по добвалению категорий, осуществляющий основную работу с БД.

        :param new_category: dict, содержащий поля, переданные в запросе + id пользователя
        :return:
        """
        user_id = new_category.get('user_id')
        name = new_category.get('name')
        parent_id = new_category.get('parent_id')
        if parent_id is None:
            parent_id = 'NULL'

        # Проверка сущестования parent_id
        if parent_id != 'NULL':
            existed_category = self.connection.execute(
                '''
                SELECT * FROM category
                WHERE id = ?
                ''',
                (parent_id,)
            )
            existed_category = existed_category.fetchone()
            if not existed_category:
                raise CategoryDoesNotExistError()

        # Проверка, что parent_id не принадлежит другому пользователю
        if parent_id != 'NULL':
            existed_category = self.connection.execute(
                '''
                SELECT * FROM category
                WHERE id = ? AND user_id = ?
                ''',
                (parent_id, user_id)
            )
            existed_category = existed_category.fetchone()
            if not existed_category:
                raise OtherUserCategory()

        # Проверка на попытку создания полной копии категории
        existed_category = self.connection.execute(
            f'''
            SELECT id, name, parent_id FROM category
            WHERE user_id = ? AND name = ? AND parent_id IS ?
            ''',
            (user_id, name, parent_id)
        )
        existed_category = existed_category.fetchone()
        if existed_category:
            new_category['id'] = dict(existed_category)['id']
            new_category.pop('user_id')
            return jsonify(new_category), 200   # TODO вынести работу с кодами в bp

        # Проверка на уникальность поля name в пределах одного пользователя
        existed_category = self.connection.execute(
            f'''
                    SELECT * FROM category
                    WHERE user_id = ? AND name = ?
                    ''',
            (user_id, name)
        )
        existed_category = existed_category.fetchone()
        if existed_category:
            raise CategoryAddingFailedError()

        # Заполнение таблицы после всех проверок
        keys = ', '.join(f'{key}' for key in new_category.keys())
        values = ', '.join(f'"{value}"' for value in new_category.values())

        try:    # TODO Отрефакторить. В helper.py есть удобная утилита для такой вставки
            self.connection.execute('PRAGMA foreign_keys = ON')
            cur = self.connection.execute(f'INSERT INTO category ({keys}) VALUES ({values})')
            instance_id = cur.lastrowid
        except sqlite.IntegrityError:
            self.connection.rollback()
            raise CategoryAddingFailedError()
        else:
            # Подготовка ответа
            new_category['id'] = instance_id
            new_category.pop('user_id')
        return jsonify(new_category), 201   # TODO вынести работу с кодами в bp

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
            raise OtherUserCategory()

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
        return jsonify(existed_category), 200   # TODO вынести работу с кодами в bp

    def _get_category(self, category):
        """
        Метод для получения параметров категории по её имени и
        идентификатору пользователя.

        :param category: параметрами запроса
        :return category: параметры запрашиваемой категории
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
        :return category: параметры запрашиваемой категории
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
        :return is_owner: True or rise CategoryAccessDeniedError
        """
        category = self._get_category_by_id(category_id)
        if user_id != category['user_id']:
            raise CategoryAccessDeniedError
        else:
            return True

    def _duplicated_name(self, name, user_id, category_id):
        """
        Метод для проверки на наличие дубликата создаваемой/редактируемой
        категории с таким же именем (проверка на дубликат).

        :param name: имя категории
        :param user_id: идентификатор пользователя
        :return is_duplicated: True/False
        """
        data = dict()
        data['name'] = name
        data['user_id'] = user_id
        try:
            category = self._get_category(data)
        except CategoryDoesNotExistError:
            return False
        else:
            if category['id'] == category_id:   # Если найденный дубликат является редактируемой категорией
                return False
            else:
                return True

    def patch(self, data, category_id, user_id):
        """
        Метод, реализующий бизнес-логику эндпоинта редактирования категории

        :param data: новые параметры категории
        :param category_id: идентификатор категории
        :param user_id: идентификатор пользователя
        :return patched: параметры отредактированной категории
        """
        # Проверка на принадлежность категории пользователю
        self._is_owner(category_id, user_id)

        # Получение полей запроса
        parent_id = data.get('parent_id', None)
        name = data.get('name', None)

        # Проверка наличия параметра "parent_id" в запросе
        if parent_id is not None:
            if parent_id > 0:
                # Проверка на принадлежность родительской категории пользователю
                self._is_owner(parent_id, user_id)
            # В случае запроса на преобразование в категорию верхнего уровня (передан undefined/None)
            else:
                data['parent_id'] = None

        # Проверка наличия/дублирование параметра "name"
        if name:
            duplicated = self._duplicated_name(name, user_id, category_id)
            if duplicated:
                raise CategoryPatchError

        is_patched = update('category', data, category_id, self.connection)
        if not is_patched:
            raise CategoryPatchError
        else:
            patched = self._get_category_by_id(category_id)
            patched.pop('user_id')
            return patched
