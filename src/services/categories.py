import sqlite3 as sqlite

from exceptions import ServiceError
from flask import jsonify


class CategoryServiceError(ServiceError):
    service = 'categories'


class CategoryNotExists(CategoryServiceError):
    pass


class OtherUserCategory(CategoryServiceError):
    pass


class CategoryAddingFailedError(CategoryServiceError):
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
                raise CategoryNotExists()

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
            return jsonify(new_category), 200

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

        try:
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
        return jsonify(new_category), 201

    def delete_category(self, category):

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
            raise CategoryNotExists()

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

        return '', 200
