from database import db
from flask import (
    Blueprint,
    request,
    jsonify
)
from flask.views import MethodView
from services.categories import (
    CategoriesService,
    CategoryDoesNotExistError,
    CategoryCreateError,
    CategoryAccessDeniedError,
    CategoryPatchError,
    CategoryFullCopyError,
    CategoryDeleteError
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
        Обработчик POST-запроса на добавление категории в дерево пользователя.

        :param user: параметры авторизации
        :return: сформированный ответ
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        data['user_id'] = user['id']

        with db.connection as con:
            service = CategoriesService(con)
            try:
                created = service.create_category(data)
            except CategoryCreateError:
                return '', 409
            except CategoryDoesNotExistError:
                return '', 404
            except CategoryAccessDeniedError:
                return '', 403
            except CategoryFullCopyError:
                existed = service.get_category(data)
                return jsonify(existed), 200, {'Content-Type': 'application/json'}
            else:
                return jsonify(created), 201, {'Content-Type': 'application/json'}

    @auth_required
    def get(self, user):
        """
        Обработчик GET-запроса на получение категории.

        :param user: параметры авторизации
        :return: сформированный ответ
        """
        data = request.json

        # Проверка на пустое тело запроса
        if not data:
            return '', 400

        data['user_id'] = user['id']

        with db.connection as con:
            service = CategoriesService(con)
            try:
                category = service.get_category(data)
            except CategoryDoesNotExistError:
                return '', 404
            except CategoryAccessDeniedError:
                return '', 403
            else:
                return jsonify(category), 200, {'Content-Type': 'application/json'}


class CategoryView(MethodView):
    """
    Класс, представляющий часть API, отвечающую за редактирование/удаление категорий.
    Каждая функция класса реализует одну из операций, результатом выполнения является код HTTP-ответа + JSON файл,
    если того требует ТЗ.
    """
    @auth_required
    def delete(self, user, category_id):
        """
        Обработчик DELETE-запроса на удаление категории.

        :param category_id: идентификатор удаляемой категории
        :param user: параметры авторизации
        :return: сформированный ответ
        """
        data = {
            'user_id': user['id'],
            'category_id': category_id
        }

        with db.connection as con:
            service = CategoriesService(con)

            try:
                service.delete_category(data)
            except CategoryAccessDeniedError:
                return '', 403
            except CategoryDoesNotExistError:
                return '', 404
            except CategoryDeleteError:
                return '', 409
            else:
                return '', 200

    @auth_required
    def patch(self, user, category_id):
        """
        Обработчик PATCH-запроса на редактирование категории.

        :param user: параметры авторизации
        :param category_id: идентификатор редактируемой категории
        :return: сформированный ответ
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
                category = service.patch_category(data, category_id, user['id'])
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
