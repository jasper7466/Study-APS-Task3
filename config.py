import os
from os import environ


class Config:
    """
    Класс для конфигурирования приложения.
    Получает параметры из переменных окружения,
    в случае их отстутсвия - применяет значения по умолчанию.
    """
    DB_CONNECTION = environ.get('DB_CONNECTION', 'example.db')
    SECRET_KEY = environ.get('SECRET_KEY', 'secret_key').encode()
