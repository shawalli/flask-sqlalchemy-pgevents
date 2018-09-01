
from flask_sqlalchemy_pgevents import PGEvents
from psycopg2_pgevents import trigger_function_installed, trigger_installed
from pytest import raises, mark

from helpers.db import create_all


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
        assert (pg._psycopg2_handle is not None)
        assert (trigger_function_installed(pg._psycopg2_handle) == True)
        assert (app.extensions.get('pgevents', None) is not None)
        assert (pg._initialized == True)

    def test_not_initialized_teardown_connection(self):
        pg = PGEvents()
        pg._psycopg2_handle = 1

        pg._teardown_connection()

        assert (pg._psycopg2_handle == 1)

    def test_teardown_connection(self, app, db):
        pg = PGEvents(app)

        pg._teardown_connection()

        assert (pg._psycopg2_handle is None)
        assert (pg._connection is None)

    def test_get_full_table_name_default_schema(self, app, db):
        class Animal(db.Model):
            __tablename__ = 'animal'

            id = db.Column(db.Integer, primary_key=True)

        pg = PGEvents(app)

        expected = 'public.animal'
        actual = pg._get_full_table_name(Animal)

        assert (expected == actual)

    def test_get_full_table_name_custom_schema(self, app, db):
        class Animal(db.Model):
            __tablename__ = 'animal'
            __table_args__ = {'schema': 'private'}
            id = db.Column(db.Integer, primary_key=True)

        pg = PGEvents(app)

        expected = 'private.animal'
        actual = pg._get_full_table_name(Animal)

        assert (expected == actual)

    def test_install_trigger_for_model(self, app, db):
        class Animal(db.Model):
            __tablename__ = 'animal'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        pg = PGEvents(app)

        trigger_installed_ = trigger_installed(pg._psycopg2_handle, 'animal')

        assert (trigger_installed_ == False)

        pg._install_trigger_for_model(Animal)

        trigger_installed_ = trigger_installed(pg._psycopg2_handle, 'animal')

        assert (trigger_installed_ == True)