import sqlite3 as sqlite


class SqliteDB:
    """
    Вспомогательный класс для упрощения работы с БД.
    """
    def __init__(self, app=None):
        self._connection = None
        self._app = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self._app = app
        self._app.teardown_appcontext(self.close_db)

    @property
    def connection(self):
        self._connect()
        return self._connection

    def _connect(self):
        connection_string = self._app.config['DB_CONNECTION']
        self._connection = sqlite.connect(
            connection_string,
            detect_types=sqlite.PARSE_DECLTYPES | sqlite.PARSE_COLNAMES
        )
        self._connection.row_factory = sqlite.Row

    def close_db(self, exception):
        if self._connection is not None:
            self._connection.close()
            self._connection = None


db = SqliteDB()
