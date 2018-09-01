
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from pytest import fixture


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

    return db_
