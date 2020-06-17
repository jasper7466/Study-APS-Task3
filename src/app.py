from blueprints.categories import bp as categories_bp
from database import db
from flask import Flask


def create_app():
    """
    Функция создания приложения
    :return: app
    """
    app = Flask(__name__)
    app.config.from_object('config.Config')
    app.register_blueprint(categories_bp, url_prefix='/categories')

    db.init_app(app)

    return app
