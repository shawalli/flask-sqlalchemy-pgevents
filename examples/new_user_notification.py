"""This single-file app demonstrates how to use Flask-SQLAlchemy-PGEvents to
listen for new user-accounts output them to the console.

Requirements (at time of creation):

attrs==18.2.0
click==6.7
Flask==1.0.2
Flask-SQLAlchemy==2.3.2
Flask-SQLAlchemy-PGEvents==0.1.0
gevent==1.3.6
greenlet==0.4.14
gunicorn==19.9.0
huey==1.10.2
itsdangerous==0.24
Jinja2==2.10
MarkupSafe==1.0
psycopg2-binary==2.7.5
psycopg2-pgevents==0.1.0
six==1.11.0
SQLAlchemy==1.2.11
SQLAlchemy-Utils==0.33.3
Werkzeug==0.14.1

"""
# Required to be imported and run before anything else
from gevent import monkey
monkey.patch_all()

from base64 import b64encode
from os import environ, urandom
from uuid import UUID

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy_pgevents import PGEvents
from huey.contrib.minimal import MiniHuey, crontab
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import expression, functions
from sqlalchemy_utils import EmailType, UUIDType


class Config:
    """This class stores configuration variables for the Flask application.
    Attributes
    ----------
    SECRET_KEY: str
        Secret used to generate security-based data, such as CSRF tokens
        (default: random 64-character string)
    SQLALCHEMY_DATABASE_URI: str
        URI of the database this app uses.
    SQLALCHEMY_TRACK_MODIFICATIONS: bool
        This variable is used by SQLAlchemy. SQLAlchemy recommends setting it
        to False if it is not explicitly needed, as the feature has a side
        effect of slowing down transactions.
    PSYCOPG2_PGEVENTS_DEBUG: bool, optional
        Whether or not to print debug logs for psycopg2-pgevents package.
    """
    SECRET_KEY: str = environ.get(
        'SECRET_KEY',
        b64encode(urandom(48)).decode('utf-8')
    )

    SQLALCHEMY_DATABASE_URI: str = environ.get(
        'DATABASE_URL',
        'postgres:///postgres'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False

    PSYCOPG2_PGEVENTS_DEBUG: bool = environ.get(
        'PSYCOPG2_PGEVENTS_DEBUG',
        False
    )


# Create app
APP = Flask(__name__)
APP.config.from_object(Config)

# Create extensions
DB = SQLAlchemy()
PG = PGEvents()
HUEY = MiniHuey()


#
# Define Models
#


class UserAccount(DB.Model):
    """Model that represents a user accuont.

    Attributes
    ----------
    __tablename__: str
        Table name, as set by Heroku Connect
    id: DB.Integer
        PostGreSQL row ID.
    first_name: DB.Text
        First name of a user.
    last_name: DB.Text
        Last name of a user.
    username: DB.Text, unqiue
        Username for user's account.
    password: DB.Text
        Password for user's account. WARNING: This is for instructional
        purposes only. Never, ever, ever, EVER store passwords in
        plaintext. Ever.
    email: sqlalchemy_utils.EmailType, unique
        Email for user's account.
    active: bool
        Whether or not the account is active.
    user_since: DB.DateTime
        Date and time that the account was created.
    """

    __tablename__ = 'useraccounts'

    id = DB.Column(DB.Integer, primary_key=True)
    first_name = DB.Column(DB.Text, nullable=False)
    last_name = DB.Column(DB.Text, nullable=False)
    username = DB.Column(DB.Text, nullable=False, unique=True)
    password = DB.Column(DB.Text, nullable=False)
    email = DB.Column(EmailType, nullable=False)
    active = DB.Column(DB.Boolean, server_default=expression.true())
    user_since = DB.Column(DB.DateTime, server_default=functions.now())

    @property
    def full_name(self) -> str:
        """Construct the user's full name.

        Returns
        -------
        str
            Full name of user account.
        """
        return '{fname} {lname}'.format(
            fname=self.first_name,
            lname=self.last_name
        )

    def __repr__(self) -> str:
        """Represent this instance as a string.

        Returns
        -------
        str
            User account represented as a string.
        """
        return (
            '<UserAccount {uname}, {email}: '
            'name:{name} active:{active}>'
        ).format(
            uname=self.username,
            email=self.email,
            name=self.full_name,
            active=self.active
        )


class Event(DB.Model):
    """Used as a simple lock for events.

    Because uniqueness is enforced on the event ID, only oen process may ever
    handle an event, no matter the number of worker processes (e.g. Gunicorn)
    or dynos (Heroku) listening for events.

    An alternative to this type of design is to limit the total number of
    instances of this process to 1. For instance, Gunicorn respects the
    environmental variable WEB_CONCURRENCY. If set to "1", Gunicorn will only
    maintain a single instance of a process.

    Attributes
    ----------
    id: int
        Record id (automatically set by SQLAlchemy)
    event_id: UUIDType
        Pgevent event UUID.
    """
    __tablename__ = 'events'

    id = DB.Column(DB.Integer, primary_key=True)
    event_id = DB.Column(UUIDType, nullable=False, unique=True)


#
# Tasks
#


@HUEY.task(crontab(minute='*'))
def pgevents_task() -> None:
    """Handle PGEvent events. Runs every minute, via Mini-Huey.

    Returns
    -------
    None

    """
    PG.handle_events()


#
# Helpers
#


def claim_event(event_id: UUID) -> bool:
    """Claim the event so that no other workers may process it. If another
    worker has already processed it, return False.

    Parameters
    ----------
    event_id: UUID
        Pgevent event UUID.

    Returns
    -------
    bool
        Whether or not the event could be claimed.
    """
    claimed = False

    try:
        DB.session.add(Event(event_id=event_id))
        DB.session.flush()
        DB.session.commit()

        claimed = True
    except IntegrityError:
        DB.session.rollback()

    return claimed


#
# Pgevent event listeners
#


@PG.listens_for(UserAccount, {'insert'})
def useraccount_event_listener(event_id: UUID, row_id: str,
                               identifier: str) -> None:
    """Handle UserAccount inserts.

    This event listener prints a message to the console whenever someone signs
    up for the site.

    Parameters
    ----------
    event_id: UUID
        PGEvent event UUID.
    row_id: str
        Row ID of the table for which this event was generated.

    Returns
    -------
    None

    """
    with APP.app_context():
        acct = UserAccount.query.filter_by(id=row_id).first()
        print('New user account created!!! {}'.format(acct))


#
# Initialize extensions
#


DB.init_app(APP)
with APP.app_context():
    DB.create_all()
PG.init_app(APP)
HUEY.start()

# We're live!
APP.run()
