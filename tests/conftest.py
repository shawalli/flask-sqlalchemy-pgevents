
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from psycopg2_pgevents import uninstall_trigger_function
from pytest import fixture

from helpers.db import create_connection


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
    db_.drop_all()

    with create_connection(db_, raw=True) as conn:
        # Ensure base pgevents trigger function is removed. By force-uninstalling,
        # all pgevent triggers (which depend on the base pgevents trigger function)
        # will be cascade-deleted.
        uninstall_trigger_function(conn, force=True)

    return db_
