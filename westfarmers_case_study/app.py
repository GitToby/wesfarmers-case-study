import contextlib
import glob
import logging
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)8s - %(message)s", level=logging.INFO)

PROJ_ROOT = Path(__file__).parent.parent.resolve()

load_dotenv(PROJ_ROOT / ".env")

USER = os.getenv('TF_VAR_wf_user')
PASS = os.getenv('TF_VAR_wf_pass')
ACCOUNT = os.getenv('TF_VAR_snowflake_account')
REGION = os.getenv('TF_VAR_snowflake_region')


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
_restricted_col_keywords = {"birth", "dob", "address", "post_code", "postcode"}

files = glob.glob(str(PROJ_ROOT / "data" / "*"))

with get_snowflake() as con:
    # upload all files in one logical session
    try:
        for file in files:
            df = pd.read_csv(file)
            table_name = file.split(os.sep)[-1].replace(".csv", "")  # personal preference
            col_types = {
                col_name: _col_type_map.get(str(col_py_type), 'varchar')
                for col_name, col_py_type in dict(df.dtypes).items()
            }

            logging.info(f"using file at {file}, copying into table with name {table_name}")

            sensitive_cols = [col for col in col_types.keys() if any(kw in col for kw in _restricted_col_keywords)]
            sensitive_col_masks = {}

            for col_name in sensitive_cols:
                # create masking policy which we can alter down the line
                mask_name = f"{col_name}_mask".upper()
                create_mask_sql = f"""
                CREATE MASKING POLICY IF NOT EXISTS {mask_name} AS (val {col_types.get(col_name)}) returns {col_types.get(col_name)} ->
                    CASE
                      WHEN current_role() IN ('WF_loader') THEN VAL
                      ELSE NULL
                    END;
                """
                con.cursor().execute(create_mask_sql)
                sensitive_col_masks[col_name] = f"WITH MASKING POLICY {mask_name}"

            col_def_strs = [
                f"{col_name} {col_type} {sensitive_col_masks.get(col_name, '')}".strip()
                for col_name, col_type in col_types.items()
            ]

            create_table_sql = (
                f"CREATE OR REPLACE TABLE {table_name} ({', '.join(col_def_strs)}) "
                " STAGE_FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1)"
                " STAGE_COPY_OPTIONS = (PURGE = TRUE)"
            )
            con.cursor().execute(create_table_sql)
            con.cursor().execute(f"PUT file://{file} @%{table_name}")
            con.cursor().execute(f"COPY INTO {table_name} from @%{table_name}")
            logging.info("loaded.")
    except Exception as e:
        logging.info(f"failed to upload... {str(e)}")
