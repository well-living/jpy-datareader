# jpy_datareader/base.py
import time
import urllib
from typing import Dict, Any, Optional

import pandas as pd
import requests

from jpy_datareader._utils import (
    RemoteDataError,
    _init_session,
)


class _BaseReader:
    """
    Base class for data readers with retry and session management.

    Parameters
    ----------
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    timeout : int, default 30
        Request timeout in seconds.
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used.
    """

    def __init__(
        self,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        if not isinstance(pause, (int, float)) or pause < 0:
            raise ValueError("'pause' must be a positive number")
        if not isinstance(timeout, int) or timeout <= 0:
            raise ValueError("'timeout' must be a positive integer")
        
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout
        self.pause_multiplier = 1
        self.session = _init_session(session)
        self.headers: Optional[Dict[str, str]] = None

    def close(self) -> None:
        """Close network session."""
        self.session.close()

    @property
    def url(self) -> str:
        """API URL - must be overridden in subclass."""
        raise NotImplementedError

    @property
    def params(self) -> Optional[Dict[str, Any]]:
        """Parameters to use in API calls."""
        return None

    def read(self) -> pd.DataFrame:
        """Read data from connector."""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def read_json(self) -> Dict[str, Any]:
        """Read data from connector and return as raw JSON."""
        try:
            response = self._get_response(self.url, params=self.params)
            return response.json()
        finally:
            self.close()
    
    def _read_one_data(self, url: str, params: Optional[Dict[str, Any]]) -> pd.DataFrame:
        """Read one data from specified URL."""
        out = self._get_response(url, params=params).json()
        return self._read_lines(out)

    def _get_response(
        self, 
        url: str, 
        params: Optional[Dict[str, Any]] = None, 
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """
        Send raw HTTP request to get requests.Response from the specified url.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Optional[Dict[str, Any]]
            Parameters passed to the URL
        headers : Optional[Dict[str, str]]
            Headers for the request
            
        Returns
        -------
        requests.Response
            Response object from the HTTP request
            
        Raises
        ------
        RemoteDataError
            If unable to retrieve data after all retry attempts
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

        # If we reach here, we have exhausted all retries.
        if params is not None and len(params) > 0:
            url = url + "?" + urllib.parse.urlencode(query=params)
        msg = f"Unable to read URL: {url}"
        if last_response_text:
            msg += f"\nResponse Text:\n{last_response_text}"

        raise RemoteDataError(msg)

    def _read_lines(self, out: Dict[str, Any]) -> pd.DataFrame:
        """
        Process JSON response into DataFrame.
        
        Parameters
        ----------
        out : Dict[str, Any]
            JSON response data
            
        Returns
        -------
        pd.DataFrame
            Processed DataFrame
        """
        rs = pd.json_normalize(out, sep="_")
        # Remove blank space character in header names
        rs = rs.assign(**{
            col.strip(): rs[col] for col in rs.columns
        }).drop(columns=rs.columns.tolist())

        # Get rid of unicode characters in index name.
        try:
            rs.index.name = rs.index.name.decode("unicode_escape").encode(
                "ascii", "ignore"
            )
        except AttributeError:
            # Python 3 string has no decode method.
            rs.index.name = rs.index.name.encode("ascii", "ignore").decode()

        return rs

    def _output_error(self, response: requests.Response) -> bool:
        """
        Handle HTTP error responses.
        
        Parameters
        ----------
        response : requests.Response
            Response object to check for errors
            
        Returns
        -------
        bool
            True if error should stop retry loop, False otherwise
        """
        # Override in subclasses for specific error handling
        return False