"""
conftest.py — pytest configuration for covid-dashboard-enhancements tests.

Patches streamlit decorators so pure functions from dashboard/app.py can be
imported and tested without a live Streamlit server.
"""
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def mock_streamlit():
    """
    Patch st.cache_data and st.cache_resource with pass-through decorators,
    and st.session_state with an empty dict.
    This allows importing pure functions from dashboard.app without Streamlit.
    """
    with patch("streamlit.cache_data", lambda *a, **kw: (lambda f: f)), \
         patch("streamlit.cache_resource", lambda *a, **kw: (lambda f: f)), \
         patch("streamlit.session_state", {}):
        yield
