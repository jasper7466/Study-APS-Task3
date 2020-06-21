from database import db
from flask import (
    Blueprint,
    request,
    jsonify
)
from flask.views import MethodView
from services.categories import (
    CategoriesService,
    CategoryAddingFailedError,
    CategoryDoesNotExistError,
    OtherUserCategory,
    CategoryAccessDeniedError,
    CategoryPatchError
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
        Метод осуществляет добавление категории в дерево пользователя.

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
            except CategoryDoesNotExistError:
                return '', 404
            except OtherUserCategory:
                return '', 403
            else:
                return new_category

    @auth_required
    def get(self, user):
        """
        Метод осуществляет получение категории из дерева пользователя.

        :param user: id авторизованного пользователя
        :return:
        """
        request_json = request.json
        request_json['user_id'] = user['id']

        with db.connection as con:
            service = CategoriesService(con)

            try:
                new_category = service.get_category(request_json)
            except CategoryDoesNotExistError:
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
        Метод осуществляет удаление категории из дерева пользователя.

        :param category_id: идентификатор удаляемой категории
        :param user: идентификатор авторизованного пользователя
        :return response: сформированный ответ
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
            except CategoryDoesNotExistError:
                return '', 404
            else:
                return deleted_category

    @auth_required
    def patch(self, user, category_id):
        """
        Обработчик PATCH-запроса на редактирование категории.

        :param user: параметры авторизации
        :param category_id: идентификатор редактируемой категории
        :return response: сформированный ответ
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        # Проверка ссылки "на себя"
        if data.get('parent_id', -1) == category_id:
            return '', 409

        with db.connection as con:
            service = CategoriesService(con)
            try:
                category = service.patch(data, category_id, user['id'])
            except CategoryDoesNotExistError:
                return '', 404
            except CategoryAccessDeniedError:
                return '', 403
            except CategoryPatchError:
                return '', 409
            else:
                return jsonify(category), 200, {'Content-Type': 'application/json'}


bp.add_url_rule('', view_func=CategoriesView.as_view('categories'))
bp.add_url_rule('/<int:category_id>', view_func=CategoryView.as_view('category'))
