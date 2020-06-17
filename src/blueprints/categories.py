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
    Класс, представляющий часть API, отвечающую за добавление/получение/редактирование/удаление категорий.
    Каждая функция класса реализует одну из операций, результатом выполнения является код HTTP-ответа + JSON файл,
    если того требует ТЗ.
    """

    @auth_required
    def post(self, user):

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


bp.add_url_rule('', view_func=CategoriesView.as_view('categories'))
