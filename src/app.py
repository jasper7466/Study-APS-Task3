from flask import Flask

from database import db


def create_app():
    """
    Функция создания приложения
    :return: app
    """
    app = Flask(__name__)
    app.config.from_object('config.Config')

    db.init_app(app)

    return app
