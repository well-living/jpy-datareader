import time
import urllib

import pandas as pd
import requests

from jpy_datareader._utils import (
    RemoteDataError,
    _init_session,
)


class _BaseReader:
    """
    Parameters
    ----------
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used.
    freq : {str, None}
        Frequency to use in select readers
    """

    def __init__(
        self,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout
        self.pause_multiplier = 1
        self.session = _init_session(session)
        self.headers = None

    def close(self):
        """Close network session"""
        self.session.close()

    @property
    def url(self):
        """API URL"""
        # must be overridden in subclass
        raise NotImplementedError

    @property
    def params(self):
        """Parameters to use in API calls"""
        return None

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()
        return self._read_lines(out)

    def _get_response(self, url, params=None, headers=None):
        """send raw HTTP request to get requests.Response from the specified url
        Parameters
        ----------
        url : str
            target URL
        params : dict or None
            parameters passed to the URL
        """
        headers = headers or self.headers
        pause = self.pause
        last_response_text = ""
        for _ in range(self.retry_count + 1):
            response = self.session.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
            if response.status_code == requests.codes.ok:
                return response

            if response.encoding:
                last_response_text = response.text.encode(response.encoding)
            time.sleep(pause)

            # Increase time between subsequent requests, per subclass.
            pause *= self.pause_multiplier

            # If our output error function returns True, exit the loop.
            if self._output_error(response):
                break

        if params is not None and len(params) > 0:
            url = url + "?" + urllib.parse.urlencode(query=params)
        msg = f"Unable to read URL: {url}"
        if last_response_text:
            msg += f"\nResponse Text:\n{last_response_text}"

        raise RemoteDataError(msg)

    def _output_error(self, out):
        """If necessary, a service can implement an interpreter for any non-200
         HTTP responses.
        Parameters
        ----------
        out: bytes
            The raw output from an HTTP request
        Returns
        -------
        boolean
        """
        return False

    def _read_lines(self, out):
        rs = pd.json_normalize(out, sep="_")
        # Needed to remove blank space character in header names
        rs.columns = list(map(lambda x: x.strip(), rs.columns.values.tolist()))

        # eStat sometimes does this awesome thing where they ...

        # Get rid of unicode characters in index name.
        try:
            rs.index.name = rs.index.name.decode("unicode_escape").encode(
                "ascii", "ignore"
            )
        except AttributeError:
            # Python 3 string has no decode method.
            rs.index.name = rs.index.name.encode("ascii", "ignore").decode()

        return rs
