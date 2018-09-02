
def create_all(db):
    db.Model.metadata.create_all(bind=db.engine)


def expose_psycopg2_connection(connection):
    connection_proxy = connection.connection
    psycopg2_connection = connection_proxy.connection

    return psycopg2_connection


class create_connection:
    def __init__(self, db, raw=False):
        self.db = db
        self.raw = raw
        self.connection = None

    def __enter__(self):
        connection = self.connection = self.db.engine.connect()
        if self.raw:
            connection = expose_psycopg2_connection(self.connection)
        return connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.connection.close()
