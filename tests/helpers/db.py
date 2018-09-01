
def create_all(db):
    db.Model.metadata.create_all(bind=db.engine)
