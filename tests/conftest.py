from os import environ

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from pytest import fixture
from sqlalchemy import MetaData
from sqlalchemy.sql import compiler

DATABASE_BASE_URL = environ.get("TEST_DATABASE_BASE_URL", "postgres://")
DATABASE = environ.get("TEST_DATABASE", "postgres")
DATABASE_DSN = "/".join([DATABASE_BASE_URL, DATABASE])


class Config:
    TESTING = True
    SQLALCHEMY_DATABASE_URI = DATABASE_DSN
    SQLALCHEMY_TRACK_MODIFICATIONS = False


@fixture
def app(request):
    """Create Flask application test fixture.

    Parameters
    ----------
    request: _pytest.fixtures.FixtureRequest
        Gives information about the test to this fixture. Provided by pytest.

    Yields
    ------
    Flask
        Test Flask application

    """
    app = Flask(request.module.__name__)

    app.config.from_object(Config)

    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    yield app

    ctx.pop()


@fixture
def db(app):
    """Empty test database and create database controller.

    Parameters
    ----------
    app: Flask
        Test Flask application to which the extension should be registered.

    Returns
    -------
    flask_sqlalchemy.SQLAlchemy
        Flask-SQLAlchemy database controller

    """
    db_ = SQLAlchemy()
    db_.init_app(app)

    # Ensure database is empty
    meta = MetaData(db_.engine)
    meta.reflect()
    meta.drop_all()

    return db_


def patched_visit_create_schema(self_, create):
    """Add IF NOT EXISTS modifier to 'CREATE SCHEMA' DDL.

    Parameters
    ----------
    create: sqlalchemy.schema.CreateSchema
        CreateSchema instance

    Returns
    -------
    str
        DDL statement

    """
    schema = self_.preparer.format_schema(create.element)
    return "CREATE SCHEMA IF NOT EXISTS " + schema


# This method must be monkey-patched since SQLAlchemy does not natively support
# IF EXISTS/IF NOT EXISTS modifers on DDL (as of 1.2.11). Since some tests test
# against non-default schema, and schema are not dropped as part of drop_all(),
# this monkey-patch allows for existing schema with the same name.
compiler.DDLCompiler.visit_create_schema = patched_visit_create_schema
