from database import db
from flask import (
    Blueprint,
    request
)
from flask.views import MethodView
from services.categories import (
    CategoriesService,
    CategoryAddingFailedError,
    CategoryNotExists,
    OtherUserCategory
)
from services.decorators import auth_required

bp = Blueprint('categories', __name__)


class CategoriesView(MethodView):
    """
    Класс, представляющий часть API, отвечающую за добавление/получение категорий.
    Каждая функция класса реализует одну из операций, результатом выполнения является код HTTP-ответа + JSON файл,
    если того требует ТЗ.
    """

    @auth_required
    def post(self, user):

        """
        Метод осуществляет добавление категории в дерево пользователя
        :param user: id авторизованного пользователя
        :return:
        """

        request_json = request.json
        request_json['user_id'] = user['id']

        with db.connection as con:
            service = CategoriesService(con)

            try:
                new_category = service.add_category(request_json)
            except CategoryAddingFailedError:
                return '', 409
            except CategoryNotExists:
                return '', 404
            except OtherUserCategory:
                return '', 403
            else:
                return new_category

    @auth_required
    def get(self, user):
        """
        Метод осуществляет получение категории из дерева пользователя
        :param user: id авторизованного пользователя
        :return:
        """
        request_json = request.json
        request_json['user_id'] = user['id']

        with db.connection as con:
            service = CategoriesService(con)

            try:
                new_category = service.get_category(request_json)
            except CategoryNotExists:
                return '', 404
            else:
                return new_category


class CategoryView(MethodView):
    """
    Класс, представляющий часть API, отвечающую за редактирование/удаление категорий.
    Каждая функция класса реализует одну из операций, результатом выполнения является код HTTP-ответа + JSON файл,
    если того требует ТЗ.
    """
    @auth_required
    def delete(self, user, category_id):
        """
        Метод осуществляет удаление категории из дерева пользователя
        :param category_id: id удаляемой категории
        :param user: id авторизованного пользователя
        :return:
        """
        category_to_delete = {
            'user_id': user['id'],
            'category_id': category_id
        }

        with db.connection as con:
            service = CategoriesService(con)

            try:
                deleted_category = service.delete_category(category_to_delete)
            except OtherUserCategory:
                return '', 403
            except CategoryNotExists:
                return '', 404
            else:
                return deleted_category


class CategoryView(MethodView):
    @auth_required
    def delete(self, user, category_id):
        """
        Метод осуществляет удаление категории из дерева пользователя
        :param category_id: id удаляемой категории
        :param user: id авторизованного пользователя
        :return:
        """
        category_to_delete = {
            'user_id': user['id'],
            'category_id': category_id
        }

        with db.connection as con:
            service = CategoriesService(con)

            try:
                deleted_category = service.delete_category(category_to_delete)
            except OtherUserCategory:
                return '', 403
            except CategoryNotExists:
                return '', 404
            else:
                return deleted_category


bp.add_url_rule('', view_func=CategoriesView.as_view('categories'))
bp.add_url_rule('/<int:category_id>', view_func=CategoryView.as_view('category'))
