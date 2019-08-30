"""`hpcflow.init_db.py`"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from hpcflow import base_db


def init_db(project, check_exists=True):
    """Get database Session.

    Parameters
    ----------
    project : Project
    check_exists : bool
        If True, an exception is raised if the database does not exist. Default
        is True. This allows us to distinguish between commands that require
        the database to already exist and those that may create it (i.e.
        `submit`).

    """

    engine = create_engine(project.db_uri, echo=False)

    if check_exists:
        try:
            engine.connect()
        except:
            raise

    # Ensure models are represented in the database (`hpcflow.models` must be
    # in-scope):
    base_db.Base.metadata.create_all(engine)
    project.ensure_db_symlink()
    Session = sessionmaker(bind=engine)

    return Session
