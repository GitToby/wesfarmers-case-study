import contextlib
import glob
import logging
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)8s - %(message)s", level=logging.INFO)

PROJ_ROOT = Path(__file__).parent.parent.parent.resolve()

load_dotenv(PROJ_ROOT / "sol_1" / ".envrc")

# not connecting? works in UI but not here - check settings for authed locations?

USER = os.getenv('TF_VAR_wf_user')
PASS = os.getenv('TF_VAR_wf_pass')
ACCOUNT = os.getenv('TF_VAR_snowflake_account')
REGION = os.getenv('TF_VAR_snowflake_region')
STAGE_NAME = 'WESTFARMERS_LOADING_STAGE'


@contextlib.contextmanager
def get_snowflake() -> snowflake.connector.SnowflakeConnection:
    _conn = snowflake.connector.connect(
        user=USER,
        password=PASS,
        # account=f"{ACCOUNT}.{REGION}",
        account="zi65592.australia-east.azure",
        database="RAWS",
        schema="WESTFARMERS",
        warehouse="LOADER",
        # tag our loader queries
        session_parameters={
            'QUERY_TAG': 'LOADING WITH PYTHON SCRIPT',
        }
    )
    yield _conn
    _conn.close()


# auto identify other column types is a wip, dates and other vars will need more intelligent selections
_col_type_map = {
    "int64": "number",
    "object": "varchar"
}

files = glob.glob(str(PROJ_ROOT / "data" / "*"))

with get_snowflake() as con:
    # upload all files in one logical session
    try:
        for file in files:
            df = pd.read_csv(file)
            table_name = file.split(os.sep)[-1].replace(".csv", "")  # personal preference
            col_py_types = dict(df.dtypes)

            logging.info(f"using file at {file}, copying into table with name {table_name}")

            col_def_strs = [
                f"{col_name} {_col_type_map.get(str(col_py_type), 'varchar')}"
                for col_name, col_py_type in col_py_types.items()
            ]

            con.cursor().execute(
                (
                    f"CREATE OR REPLACE TABLE {table_name} ({', '.join(col_def_strs)}) "
                    "STAGE_FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1)"
                    "STAGE_COPY_OPTIONS = (PURGE = TRUE)"
                )
            )
            con.cursor().execute(f"PUT file://{file} @%{table_name}")
            con.cursor().execute(f"COPY INTO {table_name} from @%{table_name}")
            logging.info("loaded.")
    except Exception as e:
        logging.info(f"failed to upload... {str(e)}")
