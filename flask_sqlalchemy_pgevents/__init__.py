
__all__ = ['PGEvents']

from collections import defaultdict, namedtuple
from typing import Callable, List, NamedTuple
import atexit

from psycopg2_pgevents import install_trigger, install_trigger_function, poll


IDENTIFIERS = set(('insert', 'update', 'delete'))


class Trigger(NamedTuple):
    target: Callable
    fn: Callable
    events: List = set()
    installed: bool = False


class PGEvents:
    def __init__(self, app=None):
        self._app = None
        self._connection = None
        self._psycopg2_connection = None
        self._triggers = defaultdict(list)
        self._initialized = False

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self._app = app

        if 'sqlalchemy' not in app.extensions:
            raise RuntimeError(
                'This extension must be initialized after Flask-SQLAlchemy')

        self._setup_conection()

        install_trigger_function(self._psycopg2_connection)

        # Install any deferred triggers
        for trigger in self._triggers.values():
            if not trigger.installed:
                self._install_trigger_for_model(trigger.target)
                trigger.installed = True

        app.extensions['pgevents'] = self

        self._initialized = True

    def listen(self, target, identifiers, fn):
        installed = False
        identifiers = set(identifiers)

        if not identifiers:
            raise ValueError('At least one identifier must be provided')

        invalid_identifiers = identifiers.difference(IDENTIFIERS)
        if invalid_identifiers:
            raise ValueError('Invalid identifiers: {}'.format(list(invalid_identifiers)))

        if self._initialized:
            self._install_trigger_for_model(target)
            installed = True

        trigger_name = self._get_full_table_name(target)

        self._triggers[trigger_name].append(Trigger(target, fn, identifiers, installed))

    def listens_for(self, target, identifiers):
        def decorate(fn):
            self.listen(target, identifiers, fn)
            return fn
        return decorate

    def _setup_conection(self):
        with self._app.app_context():
            flask_sqlalchemy = self._app.extensions['sqlalchemy']
            self._connection = flask_sqlalchemy.db.engine.connect()
            connection_proxy = self._connection.connection
            self._psycopg2_connection = connection_proxy.connection

        atexit.register(self._teardown_connection)

    def _teardown_connection(self):
        if self._connection is not None:
            with self._app.app_context():
                self._psycopg2_connection = None
                self._connection.close()
                self._connection = None

    @staticmethod
    def _get_full_table_name(model):
        table_args = getattr(model, '__table_args__', {})
        schema_name = table_args.get('schema', 'public')
        table_name = model.__tablename__

        return '{schema}.{table}'.format(schema=schema_name, table=table_name)

    def _install_trigger_for_model(self, model):
        table = self._get_full_table_name(model)
        (schema_name, table_name) = table.split('.')

        install_trigger(self._psycopg2_connection, table_name, schema=schema_name)

# TODO:???


# def notify(self):
#     if not self._initialized:
#         raise RuntimeError('Extension not initialized.')

#     for notification in poll(self._psycopg2_connection):
#         table = '{}.{}'.format(notification['schema_name'], notification['table_name'])

#         triggers = self._triggers.get(table, [])
#         if not triggers:
#             continue

#         for trigger in triggers:
#             if notification['event'].lower() not in trigger.events:
#                 continue

#         fn = notification['fn']
#         fn(notification['id'], notification['event'])
