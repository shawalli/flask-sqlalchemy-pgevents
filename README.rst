#########################
Flask-SQLAlchemy-PGEvents
#########################

.. image:: https://badge.fury.io/py/flask-sqlalchemy-pgevents.svg
    :target: https://badge.fury.io/py/flask-sqlalchemy-pgevents
.. image:: https://circleci.com/gh/shawalli/flask-sqlalchemy-pgevents.svg?style=svg
    :target: https://circleci.com/gh/shawalli/flask-sqlalchemy-pgevents
.. image:: https://coveralls.io/repos/github/shawalli/flask-sqlalchemy-pgevents/badge.svg?branch=master
    :target: https://coveralls.io/github/shawalli/flask-sqlalchemy-pgevents?branch=master
.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT

Flask-SQLAlchemy-PGEvents provides PostGreSQL eventing for Flask. It handles
setting up the underlying database, registering triggers, and polling for
events.

**************
Why Do I Care?
**************

   *I have SQLAlchemy, which supports event listeners. Why do I care about this
   extension?*

SQLAlchemy's event listening framework is great for listening to database
changes made through SQLAlchemy. However, in the real world, not every data
event that affects a database takes place through SQLAlchemy; an application
may be created from any number of packages, libraries, and modules written
in different languages and with different frameworks. If any of these
non-SQLAlchemy items modify a database, SQLAlchemy will not know, and will
therefore not notify event listeners of these changes.

With this extension, an application may be notified of events at the
*database layer*. This means that any changes made to a table are caught by
this extension and registered event listeners (for the affected table) are
called.

*******************
Why Use SQLAlchemy?
*******************

    *You just said that SQLAlchemy has nothing to do with the eventing aspect
    of this extension...So why are you using SQLAlchemy?*

Great question! SQLAlchemy is primarily used as a convenience mechanism for
creating a consistent connection to the database.

Additionally, many Flask applications use SQLAlchemy as their ORM. As such,
this extension will integrate seamlessly with any Flask applications that
use `Flask-SQLAlchemy <https://github.com/mitsuhiko/flask-sqlalchemy>`_. To
provide a consistent SQLAlchemy experience, this extension's event listener
decorator is designed to closely resemble SQLAlchemy event listener decorators.

Note
    While this extension may appear to integrate with SQLAlchemy's event
    listeners, it actually sits alongside that eventing structure. Registering
    a PGEvents event listener does not register the event listener with
    SQLAlchemy's ``event`` registrar.

********
Examples
********

See the ``examples`` directory for example use cases for this package.

************
Future Plans
************

* With a little bit of work, it should be possible to completely integrate this
  extension's event listeners into ``SQLAlchemy.event``, so that event listeners
  are functionally identical to SQLAlchemy's event listeners.

* Currently, the only supported events are after-insert and after-update.
  The ``psycopg2-pgevent`` package could be updated in coordination with this
  extension to support other `SQLAlchemy mapper events
  <http://docs.sqlalchemy.org/en/latest/orm/events.html#mapper-events>`_.

**********
References
**********

* `psycopg2-pgevents <https://github.com/shawalli/psycopg2-pgevents>`_

* `SQLAlchemy <https://bitbucket.org/zzzeek/sqlalchemy>`_

* `Flask-SQLAlchemy <https://github.com/mitsuhiko/flask-sqlalchemy>`_

**********************
Authorship and License
**********************

Written by Shawn Wallis and distributed under the MIT license.
