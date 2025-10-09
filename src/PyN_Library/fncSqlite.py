

def updateExistsTableFromDataframe(conn, df, table_name,keyColumns,ignoresUpdateColumns):
    """
    for sqlite
    Update an existing table in the SQLite database from a pandas DataFrame.
    """
    print(f"check df: {df}")

    cur = conn.cursor()
    # Get the column names from the DataFrame
    columns = list(df.columns)
    # Get the column types from the DataFrame
    types = df.dtypes
    # Get the column names and types from the database
    cur.execute("PRAGMA table_info({})".format(table_name))
    names_types = cur.fetchall()

    names_types = [(name, type) for _, name, type, _, _, _ in names_types]
    # Check if the column names are the same
    if set(columns) != set([name for name, _ in names_types]):
        raise ValueError("Column names are different between DataFrame and database")
    
    # Check if the column types are the same
    # if set(types) != set([type for _, type in names_types]):
    #     raise ValueError("Column types are different between DataFrame and database")

    columnsExludeIgnoresUpdateColumns = [name for name in columns if name not in ignoresUpdateColumns]
    # Update the table
    for i in range(len(df)):
        ValueFromKeyColumns= tuple([df.iloc[i][name] for name in keyColumns])
        sql = "UPDATE {} SET {} WHERE {}".format(
            table_name,
            ", ".join(["{} = ?".format(name) for name in columnsExludeIgnoresUpdateColumns]),
            # " AND ".join(["{} = ?".format(name) for name in keyColumns])
            " AND ".join(["{} = '{}'".format(name, value) for name, value in zip(keyColumns, ValueFromKeyColumns)])
        )
        # params = tuple(dfFilter.iloc[i]) #+ ValueFromKeyColumns
        params = tuple([df.iloc[i][name] for name in columnsExludeIgnoresUpdateColumns]) #+ ValueFromKeyColumns

        sql2 = sql.replace('?','{{}}').format(params)
        print(f"sql2: {sql2}")
        
        cur.execute(sql, params)
        conn.commit()


