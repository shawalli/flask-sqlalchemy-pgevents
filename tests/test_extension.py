
from flask_sqlalchemy_pgevents import PGEvents
from psycopg2_pgevents import trigger_function_installed, trigger_installed
from pytest import raises

from helpers.db import create_connection, create_all
from helpers.pgevents import create_pgevents


class TestExtension:
    def test_init(self, app):
        pg = PGEvents()

        assert (pg._initialized == False)

    def test_init_w_app(self, app):
        with raises(RuntimeError):
            pg = PGEvents(app)

            assert (pg._app is not None)
            assert (pg._initialized == False)

    def test_init_app_no_sqlalchemy(self, app):
        pg = PGEvents()

        with raises(RuntimeError):
            pg.init_app(app)

        assert (pg._app is not None)
        assert (pg._initialized == False)

    def test_init_app(self, app, db):
        pg = PGEvents()

        assert (pg._initialized == False)

        pg.init_app(app)

        assert (pg._app == app)
        assert (pg._connection is not None)
        assert (pg._psycopg2_connection is not None)

        with create_connection(db, raw=True) as conn:
            trigger_function_installed_ = trigger_function_installed(conn)
            assert (trigger_function_installed_ == True)

        assert (app.extensions.get('pgevents', None) is not None)
        assert (pg._initialized == True)

    def test_not_initialized_teardown_connection(self):
        pg = PGEvents()
        pg._psycopg2_connection = 1

        pg._teardown_connection()

        assert (pg._psycopg2_connection == 1)

    def test_teardown_connection(self, app, db):
        pg = PGEvents(app)

        pg._teardown_connection()

        assert (pg._psycopg2_connection is None)
        assert (pg._connection is None)

        # Set to False so that teardown() isn't called as part of cleanup
        pg._initialized = False

    def teardown_not_initialized(self):
        pg = PGEvents()

        pg._psycopg2_connection = 1

        pg.teardown()

        assert (pg._psycopg2_connection == 1)

    def test_teardown(self, app, db):
        pg = PGEvents(app)

        pg.teardown()

        assert (pg._psycopg2_connection is None)
        assert (pg._connection is None)

    def test_get_full_table_name_default_schema(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'

            id = db.Column(db.Integer, primary_key=True)

        with create_pgevents(app) as pg:
            expected = 'public.widget'
            actual = pg._get_full_table_name(Widget)

            assert (expected == actual)

    def test_get_full_table_name_custom_schema(self, app, db):
        class Gadget(db.Model):
            __tablename__ = 'gadget'
            __table_args__ = {'schema': 'private'}
            id = db.Column(db.Integer, primary_key=True)

        with create_pgevents(app) as pg:
            expected = 'private.gadget'
            actual = pg._get_full_table_name(Gadget)

            assert (expected == actual)

    def test_install_trigger_for_model(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        with create_pgevents(app) as pg:
            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == False)

                pg._install_trigger_for_model(Widget)

                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

    def test_init_app_deferred_trigger(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def callback(record_id, identifier):
            pass

        with create_pgevents() as pg:
            pg.listen(Widget, {'insert'}, callback)

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)
            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == False)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert'})
            assert (trigger.callback == callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == False)

            pg.init_app(app)

            assert (trigger.installed == True)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)
