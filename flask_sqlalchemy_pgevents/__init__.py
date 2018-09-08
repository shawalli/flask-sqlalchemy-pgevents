"""This module manages the Flask-SQLAlchemy-PGEvents extension. """
__all__ = ['PGEvents']

from collections import defaultdict, namedtuple
from typing import Callable, List, Optional, Set
import atexit

from flask import Flask
from flask_sqlalchemy.model import Model
from psycopg2.extensions import connection as Psycopg2Connection
from psycopg2_pgevents import install_trigger, install_trigger_function, poll, register_event_channel, \
    set_debug, unregister_event_channel
from sqlalchemy.engine.base import Connection as SQLAlchemyConnection
import attr

from flask_sqlalchemy_pgevents.__about__ import __author__, __copyright__, __email__, __license__, __summary__, \
    __title__, __uri__, __version__

IDENTIFIERS = {'insert', 'update', 'delete'}


@attr.s(auto_attribs=True)
class Trigger:
    """Dataclass for PGEvent triggers.

    Attributes
    ----------
    target: Callable
        SQLAlchemy model class for which to listen.
    callback: Callable
        Method to call when an event matches this trigger.
    events: set
        Event or events that this trigger should listen for.
    installed:
        Whether or not the trigger is installed.
    """
    target: Callable
    callback: Callable
    events: Set = set()
    installed: bool = False


class PGEvents:
    """PGEvents extension."""

    def __init__(self, app: Optional[Flask]=None) -> None:
        """Initialize the extension.

        Parameters
        ----------
        app: Flask, optional
            The application to which this extension will be registered.

        """
        self._app = None  # type: Optional[Flask]
        self._connection = None  # type: Optional[SQLAlchemyConnection]
        self._psycopg2_connection = None  # type: Optional[Psycopg2Connection]
        self._triggers = defaultdict(list)  # type: dict
        self._initialized = False  # type: bool

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        """Initialize the extension against an application.

        Parameters
        ----------
        app: Flask
            The application to which this extension will be registered.

        Returns
        -------
        None

        """
        self._app = app

        if 'sqlalchemy' not in app.extensions:
            raise RuntimeError(
                'This extension must be initialized after Flask-SQLAlchemy')

        self._setup_conection()

        # Initialize psycopg2-pgevents
        pgevents_debug = app.config.get('PSYCOPG2_PGEVENTS_DEBUG', False)
        set_debug(pgevents_debug)

        install_trigger_function(self._psycopg2_connection)

        # Install any deferred triggers
        for table_triggers in self._triggers.values():
            for trigger in table_triggers:
                if not trigger.installed:
                    self._install_trigger_for_model(trigger.target)
                    trigger.installed = True

        register_event_channel(self._psycopg2_connection)

        app.extensions['pgevents'] = self

        self._initialized = True

        atexit.register(self.teardown)

    def teardown(self) -> None:
        """Teardown the extension.

        Returns
        -------
        None

        """
        if self._initialized:
            unregister_event_channel(self._psycopg2_connection)

            self._teardown_connection()
        self._initialized = False

    def _setup_conection(self) -> None:
        """Set up the database connection.

        Returns
        -------
        None

        """
        with self._app.app_context():  # type: ignore
            flask_sqlalchemy = self._app.extensions['sqlalchemy']  # type: ignore
            self._connection = flask_sqlalchemy.db.engine.connect()
            connection_proxy = self._connection.connection
            self._psycopg2_connection = connection_proxy.connection

    def _teardown_connection(self) -> None:
        """Teardown the database connection.

        Returns
        -------
        None

        """
        if self._connection is not None:
            with self._app.app_context():  # type: ignore
                self._psycopg2_connection = None
                self._connection.close()
                self._connection = None

    @staticmethod
    def _get_full_table_name(model: Model) -> str:
        """Parse the SQLAlchemy model for the fully-resolved table name.

        Parameters
        ----------
        model: flask_sqlalchemy.model.Model
            Model whose name should be resolved.

        Returns
        -------
        str
            Fully-resolved table name, in the form "<SCHEMA>.<TABLE>".

        """
        table_args = getattr(model, '__table_args__', {})
        schema_name = table_args.get('schema', 'public')
        table_name = model.__tablename__

        return '{schema}.{table}'.format(schema=schema_name, table=table_name)

    def _install_trigger_for_model(self, model: Model) -> None:
        """Install a trigger for the given model.

        Parameters
        ----------
        model: flask_sqlalchemy.model.Model
            Model to which a trigger should be installed.

        Returns
        -------
        None

        """
        table = self._get_full_table_name(model)
        (schema_name, table_name) = table.split('.')

        install_trigger(self._psycopg2_connection, table_name, schema=schema_name)

    def listen(self, target: Model, identifiers: Set, fn: Callable) -> None:
        """Listen to PGEvents events for a given model.

        This method's signature mirrors the `sqlalchemy.event.listen` method for
        consistency.

        Parameters
        ----------
        target: flask_sqlalchemy.model.Model
            SQLAlchemy model class for which to listen.
        identifiers: set
            Event or events that this trigger should listen for. Should be one
            of "insert", "update", or "delete".
        fn: Callable
            Method to call when an event matches this trigger.

        Returns
        -------
        None

        """
        installed = False

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

    def listens_for(self, target: Model, identifiers: Set) -> Callable:
        """Decorate a function as a callback for one or several PGEvents events.

        This method's signature mirrors the `sqlalchemy.event.listen` method for
        consistency.

        Parameters
        ----------
        target: flask_sqlalchemy.model.Model
            SQLAlchemy model class for which to listen.
        identifiers: set
            Event or events that this trigger should listen for. Should be one
            of "insert", "update", or "delete".
        fn: Callable
            Method to call when an event matches this trigger.

        Returns
        -------
        None

        """
        def decorate(fn):
            self.listen(target, identifiers, fn)
            return fn
        return decorate

    def handle_events(self, timeout: float=0.0) -> None:
        """Handle PGEvents events, according to registered triggers.

        Parameters
        ----------
        timeout: float
            Number of seconds to block when polling for events. A value of 0.0
            sets the method as non-blocking.

        Raises
        ------
        RuntimeError
            Raises if the extension has not yet been initialized.

        Returns
        -------
        None

        """
        if not self._initialized:
            raise RuntimeError('Extension not initialized.')

        for event in poll(self._psycopg2_connection, timeout=timeout):
            table = '{}.{}'.format(event.schema_name, event.table_name)

            triggers = self._triggers.get(table, [])
            if not triggers:
                continue

            for trigger in triggers:
                if event.type.lower() not in trigger.events:
                    continue

                trigger.callback(event.id, event.row_id, event.type)
