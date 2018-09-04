
from flask_sqlalchemy_pgevents import PGEvents
from psycopg2_pgevents import trigger_installed
from pytest import raises
from sqlalchemy.schema import CreateSchema

from helpers.db import create_connection, create_all
from helpers.pgevents import create_pgevents


class TestApi:
    def test_listen_no_identifier(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def callback(record_id, identifier):
            pass
        with create_pgevents(app) as pg:
            with raises(ValueError):
                pg.listen(Widget, set(), callback)

            assert ('public.widget' not in pg._triggers)

    def test_listen_invalid_identifier(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def callback(record_id, identifier):
            pass
        with create_pgevents(app) as pg:
            with raises(ValueError):
                pg.listen(Widget, {'insert', 'upsert'}, callback)

            assert ('public.widget' not in pg._triggers)

    def test_listen_not_initialized(self, app, db):
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

    def test_listen(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def callback(record_id, identifier):
            pass

        with create_pgevents(app) as pg:
            pg.listen(Widget, {'insert'}, callback)

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)
            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert'})
            assert (trigger.callback == callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

    def test_listen_one_model_multiple_triggers(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def upsert_callback(record_id, identifier):
            pass

        def delete_callback(record_id, identifier):
            pass

        with create_pgevents(app) as pg:
            pg.listen(Widget, {'insert', 'update'}, upsert_callback)
            pg.listen(Widget, {'delete'}, delete_callback)

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 2)

            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert', 'update'})
            assert (trigger.callback == upsert_callback)

            trigger = pg._triggers['public.widget'][1]
            assert (trigger.installed == True)
            assert (trigger.target == Widget)
            assert (trigger.events == {'delete'})
            assert (trigger.callback == delete_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

    def test_listen_multiple_models(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        db.session.execute(CreateSchema('private'))
        db.session.commit()

        class Gadget(db.Model):
            __tablename__ = 'gadget'
            __table_args__ = {'schema': 'private'}
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def widget_callback(record_id, identifier):
            pass

        def gadget_callback(record_id, identifier):
            pass

        with create_pgevents(app) as pg:
            pg.listen(Widget, {'insert'}, widget_callback)
            pg.listen(Gadget, {'delete'}, gadget_callback)

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)

            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert'})
            assert (trigger.callback == widget_callback)

            assert ('private.gadget' in pg._triggers)
            assert (len(pg._triggers['private.gadget']) == 1)

            trigger = pg._triggers['private.gadget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Gadget)
            assert (trigger.events == {'delete'})
            assert (trigger.callback == gadget_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

                trigger_installed_ = trigger_installed(conn, 'gadget', schema='private')
                assert (trigger_installed_ == True)

    def test_listen_mixed(self, app, db):
        class Widget(db.Model):
            __tablename__ = 'widget'
            id = db.Column(db.Integer, primary_key=True)

        db.session.execute(CreateSchema('private'))
        db.session.commit()

        class Gadget(db.Model):
            __tablename__ = 'gadget'
            __table_args__ = {'schema': 'private'}
            id = db.Column(db.Integer, primary_key=True)

        create_all(db)

        def widget_callback(record_id, identifier):
            pass

        def gadget_callback(record_id, identifier):
            pass

        with create_pgevents() as pg:
            pg.listen(Widget, {'insert'}, widget_callback)

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)

            widget_trigger = pg._triggers['public.widget'][0]
            assert (widget_trigger.installed == False)
            assert (widget_trigger.target == Widget)
            assert (widget_trigger.events == {'insert'})
            assert (widget_trigger.callback == widget_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == False)

            pg.init_app(app)

            assert (widget_trigger.installed == True)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

            pg.listen(Gadget, {'delete'}, gadget_callback)

            assert ('private.gadget' in pg._triggers)
            assert (len(pg._triggers['private.gadget']) == 1)

            trigger = pg._triggers['private.gadget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Gadget)
            assert (trigger.events == {'delete'})
            assert (trigger.callback == gadget_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'gadget', schema='private')
                assert (trigger_installed_ == True)

    def test_listens_for_not_initialized(self, app, db):
        with create_pgevents() as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            @pg.listens_for(Widget, {'insert'})
            def callback(record_id, identifier):
                pass

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

    def test_listens_for(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            @pg.listens_for(Widget, {'insert'})
            def callback(record_id, identifier):
                pass

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)
            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert'})
            assert (trigger.callback == callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

    def test_listens_for_mixed(self, app, db):
        with create_pgevents() as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            db.session.execute(CreateSchema('private'))
            db.session.commit()

            class Gadget(db.Model):
                __tablename__ = 'gadget'
                __table_args__ = {'schema': 'private'}
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            @pg.listens_for(Widget, {'insert'})
            def widget_callback(record_id, identifier):
                pass

            assert ('public.widget' in pg._triggers)
            assert (len(pg._triggers['public.widget']) == 1)
            trigger = pg._triggers['public.widget'][0]
            assert (trigger.installed == False)
            assert (trigger.target == Widget)
            assert (trigger.events == {'insert'})
            assert (trigger.callback == widget_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == False)

            pg.init_app(app)

            assert (trigger.installed == True)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'widget')
                assert (trigger_installed_ == True)

            @pg.listens_for(Gadget, {'delete'})
            def gadget_callback(record_id, identifier):
                pass

            assert ('private.gadget' in pg._triggers)
            assert (len(pg._triggers['private.gadget']) == 1)

            trigger = pg._triggers['private.gadget'][0]
            assert (trigger.installed == True)
            assert (trigger.target == Gadget)
            assert (trigger.events == {'delete'})
            assert (trigger.callback == gadget_callback)

            with create_connection(db, raw=True) as conn:
                trigger_installed_ = trigger_installed(conn, 'gadget', schema='private')
                assert (trigger_installed_ == True)

    def test_handle_events_not_initialized(self):
        with create_pgevents() as pg:
            with raises(RuntimeError):
                pg.handle_events()

    def test_handle_events_no_events(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            widget_callback_called = 0

            @pg.listens_for(Widget, {'insert'})
            def widget_callback(record_id, identifier):
                nonlocal widget_callback_called
                widget_callback_called += 1

            pg.handle_events()

            assert (widget_callback_called == 0)

    def test_handle_events_one_table_one_event(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            widget_callback_called = 0

            @pg.listens_for(Widget, {'insert'})
            def widget_callback(record_id, identifier):
                nonlocal widget_callback_called
                widget_callback_called += 1

            db.session.add(Widget())
            db.session.commit()

            pg.handle_events()

            assert (widget_callback_called == 1)

    def test_handle_events_one_table_multiple_events(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            widget_callback_called = 0

            @pg.listens_for(Widget, {'insert'})
            def widget_callback(record_id, identifier):
                nonlocal widget_callback_called
                widget_callback_called += 1

            db.session.add(Widget())
            db.session.add(Widget())
            db.session.add(Widget())
            db.session.commit()

            pg.handle_events()

            assert (widget_callback_called == 3)

    def test_handle_events_one_table_multiple_callbacks(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)
                label = db.Column(db.Text)

            create_all(db)

            widget_insert_callback_called = 0
            widget_upsert_callback_called = 0

            @pg.listens_for(Widget, {'insert'})
            def widget_insert_callback(record_id, identifier):
                nonlocal widget_insert_callback_called
                widget_insert_callback_called += 1

            @pg.listens_for(Widget, {'insert', 'update'})
            def widget_upsert_callback(record_id, identifier):
                nonlocal widget_upsert_callback_called
                widget_upsert_callback_called += 1

            widget = Widget(label='foo')
            db.session.add(widget)
            db.session.commit()

            widget.label = 'bar'
            db.session.add(widget)
            db.session.commit()

            pg.handle_events()

            assert(widget_insert_callback_called == 1)
            assert(widget_upsert_callback_called == 2)

    def test_handle_events_multiple_tables_events(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            db.session.execute(CreateSchema('private'))
            db.session.commit()

            class Gadget(db.Model):
                __tablename__ = 'gadget'
                __table_args__ = {'schema': 'private'}
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            widget_callback_called = 0
            gadget_callback_called = 0

            @pg.listens_for(Widget, {'insert'})
            def widget_callback(record_id, identifier):
                nonlocal widget_callback_called
                widget_callback_called += 1

            @pg.listens_for(Gadget, {'insert'})
            def gadget_callback(record_id, identifier):
                nonlocal gadget_callback_called
                gadget_callback_called += 1

            db.session.add(Widget())
            db.session.add(Gadget())
            db.session.commit()

            pg.handle_events()

            assert (widget_callback_called == 1)
            assert (gadget_callback_called == 1)

    def test_handle_events_no_triggers(self, app, db):
        with create_pgevents(app) as pg:
            class Widget(db.Model):
                __tablename__ = 'widget'
                id = db.Column(db.Integer, primary_key=True)

            db.session.execute(CreateSchema('private'))
            db.session.commit()

            class Gadget(db.Model):
                __tablename__ = 'gadget'
                __table_args__ = {'schema': 'private'}
                id = db.Column(db.Integer, primary_key=True)

            create_all(db)

            widget_callback_called = 0

            def widget_callback(record_id, identifier):
                nonlocal widget_callback_called
                widget_callback_called += 1

            db.session.add(Gadget())
            db.session.commit()

            pg.handle_events()

            assert (widget_callback_called == 0)
