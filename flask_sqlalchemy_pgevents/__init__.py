
__all__ = ['PGEvents']

from collections import defaultdict, namedtuple
from typing import Callable, List, NamedTuple
import atexit

from psycopg2_pgevents import install_trigger, install_trigger_function, poll


class Trigger(NamedTuple):
    target: Callable
    fn: Callable
    events: List = list()
    installed: bool = False


class PGEvents:
    def __init__(self, app=None):
        self._app = None
        self._connection = None
        self._psycopg2_handle = None
        self._triggers = dict()
        self._initialized = False

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self._app = app

        if 'sqlalchemy' not in app.extensions:
            raise RuntimeError(
                'This extension must be initialized after Flask-SQLAlchemy')

        self._setup_conection()

        install_trigger_function(self._psycopg2_handle)

        # Install any deferred triggers
        for trigger in self._triggers.values():
            if not trigger.installed:
                self._install_trigger_for_model(trigger.target)
                trigger.installed = True

        app.extensions['pgevents'] = self

        self._initialized = True

    def listen(self, target, identifier, fn):
        installed = False
        events = identifier.split('|')

        if self._initialized:
            self._install_trigger_for_model(target)
            installed = True

        trigger_name = self._get_full_table_name(target)

        self._triggers[trigger_name] = Trigger(target, events, fn, installed)

    def listens_for(self, target, identifier):
        def decorate(fn):
            self.listen(target, identifier, fn)
            return fn
        return decorate

    def _setup_conection(self):
        with self._app.app_context():
            flask_sqlalchemy = self._app.extensions['sqlalchemy']
            self._connection = flask_sqlalchemy.db.engine.connect()
            self._psycopg2_handle = self._connection.connection

        atexit.register(self._teardown_connection)

    def _teardown_connection(self):
        if self._connection is not None:
            with self._app.app_context():
                self._psycopg2_handle = None
                self._connection.close()
                self._connection = None

    @staticmethod
    def _get_full_table_name(model):
        table_args = getattr(model, '__table_args__', {})
        schema_name = table_args.get('schema', 'public')
        table_name = model.__tablename__

        return '{}.{}'.format(schema_name, table_name)

    def _install_trigger_for_model(self, model):
        table = self._get_full_table_name(model)
        (schema_name, table_name) = table.split('.')

        install_trigger(self._psycopg2_handle, table_name, schema=schema_name)

# TODO:???


# def notify(self):
#     if not self._initialized:
#         raise RuntimeError('Extension not initialized.')

#     for notification in poll(self._psycopg2_handle):
#         table = '{}.{}'.format(notification['schema_name'], notification['table_name'])

#         trigger = self._triggers.get(table, None)
#         if trigger is None:
#             continue

#         if notification['event'].lower() not in trigger.events:
#             continue

#         fn = notification['fn']
#         fn(notification['id'], notification['event'])
