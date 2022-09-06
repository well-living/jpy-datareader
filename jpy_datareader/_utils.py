import requests


class RemoteDataError(IOError):
    pass


def _init_session(session):
    if session is None:
        session = requests.Session()
    else:
        if not isinstance(session, requests.Session):
            raise TypeError("session must be a request.Session")
    return session
