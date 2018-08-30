from collections import defaultdict, namedtuple
from typing import Callable, List, NamedTuple
import atexit

from huey.contrib.minimal import MiniHuey
from psycopg2_pgevents import install_trigger, install_trigger_function, poll


class Trigger(NamedTuple):
    target: Callable
    events: List = list()
    fn: Callable
    installed: bool = False


class PGEvents:
    def __init__(self, app=None):
        self.__app = None
        self.__connection = None
        self.__psycopg2_handle = None
        self.__triggers = dict()
        self.__initialized = False

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.__app = app

        if not hasattr(app, 'extensions'):
            app.extensions = {}

        if 'sqlalchemy' not in app.extensions:
            raise RuntimeError(
                'This extension must be initialized after Flask-SQLAlchemy')

        self.__setup_conection()

        install_trigger_function(self.__psycopg2_handle)

        # Install any deferred triggers
        for trigger in self.__triggers.values():
            if not trigger.installed:
                self.__install_trigger_for_model(trigger.target)
                trigger.installed = True

        # TODO: setup Huey
        self.__scheduler = MiniHuey()

        app.extensions['pgevents'] = self

        self.__initialized = True

    def listen(self, target, identifier, fn):
        installed = False
        events = identifier.split('|')

        if self.__initialized:
            self.__install_trigger_for_model(target)
            installed = True

        trigger_name = self.__get_full_table_name(target)

        self.__triggers[trigger_name] = Trigger(target, events, fn, installed)

    def listens_for(self, target, identifier):
        def decorate(fn):
            self.listen(target, identifier, fn)
            return fn
        return decorate

    def __setup_conection(self):
        with self.__app.app_context():
            db = self.__app.extensions.get('sqlalchemy', None)
            self.__connection = db.engine.connect()
            self.__psycopg2_handle = self.__connection.connection

        atexit.register(self.__teardown_connection)

    def __teardown_connection(self):
        if self.__connection is not None:
            with self.__app.app_context:
                self.__connection.close()
                self.__psycopg2_handle = None
                self.__connection = None

    @staticmethod
    def __get_full_table_name(model):
        table_args = getattr(model, '__table_args__', {})
        schema_name = table_args.get('schema', 'public')
        table_name = model.__tablename__

        return '{}.{}'.format(schema_name, table_name)

    def __install_trigger_for_model(self, model):
        table = self.__get_full_table_name(model)
        (schema_name, table_name) = table.split('.')

        install_trigger(self.__psycopg2_handle, table_name, schema=schema_name)

# TODO:???


def notify(self):
    if not self.__initialized:
        raise RuntimeError('Extension not initialized.')

    for notification in poll(self.__psycopg2_handle):
        table = '{}.{}'.format(notification['schema_name'], notification['table_name'])

        trigger = self.__triggers.get(table, None)
        if trigger is None:
            continue

        if notification['event'].lower() not in trigger.events:
            continue

        fn = notification['fn']
        fn(notification['id'], notification['event'])
