from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from pytest import fixture
from sqlalchemy import MetaData
from sqlalchemy.sql import compiler


class Config:
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2:///test'
    SQLALCHEMY_TRACK_MODIFICATIONS = False


@fixture
def app(request):
    app = Flask(request.module.__name__)

    app.config.from_object(Config)

    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    yield app

    ctx.pop()


@fixture
def db(app):
    """Session-wide test database."""
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


# THis method must be monkey-patched since SQLAlchemy does not natively support
# IF EXISTS/IF NOT EXISTS modifers on DDL (as of 1.2.11). Since some tests test
# against non-default schema, and schema are not dropped as part of drop_all(),
# this monkey-patch allows for existing schema with the same name.
compiler.DDLCompiler.visit_create_schema = patched_visit_create_schema
