import os
import sqlite3

from csvs_to_sqlite.utils import (
    LoadCsvError,
    LookupTable,
    PathOrURL,
    add_index,
    apply_dates_and_datetimes,
    apply_shape,
    best_fts_version,
    csvs_from_paths,
    generate_and_populate_fts,
    load_csv,
    refactor_dataframes,
    table_exists,
    drop_table,
    to_sql_with_foreign_keys,
)

def csv2db(path, dbname):

    db_existed = os.path.exists(dbname)
    conn = sqlite3.connect(dbname)

    dataframes = []
    sql_type_overrides = None

    separator = ","
    skip_errors = False
    quoting = 0
    shape = None
    just_strings = False
    table = None
    filename_column = None
    fixed_columns = []
    fixed_columns_int = []
    fixed_columns_float = []
    date = []
    datetime = []
    datetime_format = []
    extract_columns = []
    no_fulltext_fks = False
    replace_tables = False
    primary_key = None
    fts = None
    no_index_fks = False
    index = []


    try:
        df = load_csv(
            path, separator, skip_errors, quoting, shape, just_strings=just_strings
        )
        name = os.path.basename(path)
        df.table_name = "dataset"
        if filename_column:
            df[filename_column] = name
            if shape:
                shape += ",{}".format(filename_column)
        if fixed_columns:
            for colname, value in fixed_columns:
                df[colname] = value
                if shape:
                    shape += ",{}".format(colname)
        if fixed_columns_int:
            for colname, value in fixed_columns_int:
                df[colname] = value
                if shape:
                    shape += ",{}".format(colname)
        if fixed_columns_float:
            for colname, value in fixed_columns_float:
                df[colname] = value
                if shape:
                    shape += ",{}".format(colname)
        sql_type_overrides = apply_shape(df, shape)
        apply_dates_and_datetimes(df, date, datetime, datetime_format)
        dataframes.append(df)
    except LoadCsvError as e:
        return "Could not load {}: {}".format(path, e)

    # Use extract_columns to build a column:(table,label) dictionary
    foreign_keys = {}
    for col in extract_columns:
        bits = col.split(":")
        if len(bits) == 3:
            foreign_keys[bits[0]] = (bits[1], bits[2])
        elif len(bits) == 2:
            foreign_keys[bits[0]] = (bits[1], "value")
        else:
            foreign_keys[bits[0]] = (bits[0], "value")

    # Now we have loaded the dataframes, we can refactor them
    created_tables = {}
    refactored = refactor_dataframes(
        conn, dataframes, foreign_keys, not no_fulltext_fks
    )
    for df in refactored:
        # This is a bit trickier because we need to
        # create the table with extra SQL for foreign keys
        if replace_tables and table_exists(conn, df.table_name):
            drop_table(conn, df.table_name)
        if table_exists(conn, df.table_name):
            df.to_sql(df.table_name, conn, if_exists="append", index=False)
        else:
            to_sql_with_foreign_keys(
                conn,
                df,
                df.table_name,
                foreign_keys,
                sql_type_overrides,
                primary_keys=primary_key,
                index_fks=not no_index_fks,
            )
            created_tables[df.table_name] = df
        if index:
            for index_defn in index:
                add_index(conn, df.table_name, index_defn)

    conn.close()