from flask_sqlalchemy_pgevents import PGEvents


class create_pgevents:
    def __init__(self, app=None):
        self.pgevents = PGEvents(app)

    def __enter__(self):
        return self.pgevents

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pgevents._teardown_connection()
