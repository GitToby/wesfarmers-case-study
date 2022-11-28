from unittest.mock import MagicMock

import pandas as pd
import pytest as pytest
import snowflake.connector

from westfarmers_case_study.app import do_table_load


@pytest.fixture
def mock_db_session(monkeypatch):
    mock_conn: snowflake.connector.SnowflakeConnection = MagicMock()
    monkeypatch.setattr("westfarmers_case_study.app.get_snowflake", lambda *args: mock_conn)
    return mock_conn


def test_do_table_load(mock_db_session):
    df = pd.DataFrame(
        {
            "A": range(10),
            "B": list("qwertyuiop"),
            "dob": range(10),
        }
    )
    mock_cur = MagicMock()
    mock_db_session.cursor = MagicMock(side_effect=mock_cur)

    do_table_load(df, "mock_table", "/mock/path", mock_db_session)

    assert mock_cur.call_count == 4
