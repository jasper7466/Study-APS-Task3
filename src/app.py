from flask import Flask

from database import db
from blueprints.register import bp as register_bp

def create_app():
    """
    Функция создания приложения
    :return: app
    """
    app = Flask(__name__)
    app.config.from_object('config.Config')
    app.register_blueprint(register_bp, url_prefix='/register')

    db.init_app(app)

    return app
