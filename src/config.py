import os


class Config:
    """
    Класс для конфигурирования приложения.
    Получает параметры из переменных окружения,
    в случае их отстутсвия - применяет значения по умолчанию.
    """
    DB_CONNECTION = os.getenv('DB_CONNECTION', '../example.db')
    SECRET_KEY = os.getenv('SECRET_KEY', 'secret_key').encode()
