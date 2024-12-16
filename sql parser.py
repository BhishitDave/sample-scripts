spark = SparkSession.builder.getOrCreate()

# Sample input DataFrame with a column named "query"
data = [
    ("SELECT a.col1, b.col2 FROM tableA a JOIN tableB b ON a.id = b.id WHERE a.col3 = 'xyz'",),
    ("SELECT x, y FROM tableC WHERE z = 100",),
    ("SELECT * FROM tableD d JOIN tableE e ON d.key = e.key AND d.category = e.category",),
]
df = spark.createDataFrame(data, ["query"])

def parse_sql_with_sqlparse(query_str: str):
    """
    Parses a single SQL statement using sqlparse to extract:
      - tables in FROM or JOIN
      - original query
      - columns used in join (ON clauses)
      - columns in WHERE clause

    Returns a tuple of 4 strings:
      (tables_in_from_join, original_query, join_columns, where_columns)
    """
    if not query_str:
        return ("", "", "", "")
    
    # Parse the query string
    parsed_statements = sqlparse.parse(query_str)
    if not parsed_statements:
        return ("", query_str, "", "")

    stmt = parsed_statements[0]  # Assume single statement
    
    tables = []
    join_columns = []
    where_columns = []

    # We’ll traverse the top-level tokens to find:
    # - FROM and JOIN clauses to get table references
    # - WHERE clause to parse out columns
    # - ON clauses for join columns

    # sqlparse organizes tokens hierarchically. For each token:
    #   - Check if it's a "FROM" or "JOIN" token (or part of a segment that has them).
    #   - For the WHERE clause, find the Where token block.

    # Helper function: Extract table names from a token (which might be an Identifier or IdentifierList)
    def extract_tables(token):
        extracted = []
        if isinstance(token, Identifier):
            # token.get_real_name() can sometimes return the alias or actual name
            # token.get_name() is the alias if present
            # token.get_parent_name() might be the schema name
            # We'll store the raw string for simplicity:
            extracted.append(token.value)
        elif isinstance(token, IdentifierList):
            for t in token.get_identifiers():
                extracted.append(t.value)
        return extracted

    # Helper function: Extract columns from ON or WHERE comparisons
    # e.g. a.id = b.id  => we might add 'a.id' and 'b.id' to the list
    def extract_columns_from_comparison(comparison_token):
        comp_cols = []
        if isinstance(comparison_token, Comparison):
            # The left and right parts of the comparison, e.g. a.id = b.id
            left = comparison_token.left
            right = comparison_token.right
            # left.value might be "a.id"
            # right.value might be "b.id"
            comp_cols.append(left.value.strip())
            comp_cols.append(right.value.strip())
        return comp_cols

    where_clause = None  # Will store a reference to the WHERE block if present

    # Iterate top-level tokens in the statement
    for token in stmt.tokens:
        # Skip whitespace, punctuation
        if token.is_whitespace or token.ttype is Whitespace:
            continue

        # 1. Identify FROM or JOIN sections, parse out table references
        if token.ttype is Keyword and token.value.upper() in ("FROM", "JOIN"):
            # The next token(s) should contain table references
            # Usually the next significant token is an Identifier/IdentifierList
            pass  # We'll handle this logic in the "FROM" / "JOIN" blocks below

        if token.is_group:
            # If it's a group (like IdentifierList or Where block), we might want to dive deeper
            # For example, a FROM clause might be in a group, or a Where clause is a Where object
            if isinstance(token, Where):
                where_clause = token  # store for later parsing
            else:
                # Dive deeper into sub-tokens to find FROM/JOIN references
                for sub in token.tokens:
                    if sub.match(Keyword, ["FROM", "JOIN"]):
                        # Next token (or tokens) might be the table references
                        # We can attempt to parse the sibling tokens after FROM/JOIN
                        idx = token.tokens.index(sub)
                        # The next token(s) might be the tables or an IdentifierList
                        if idx + 1 < len(token.tokens):
                            next_token = token.tokens[idx + 1]
                            tables.extend(extract_tables(next_token))

                    # If there's an ON block for a JOIN, we can parse columns
                    if sub.match(Keyword, ["ON"]):
                        # The next token might be a parenthesis block or a direct comparison
                        on_idx = token.tokens.index(sub)
                        if on_idx + 1 < len(token.tokens):
                            on_condition_token = token.tokens[on_idx + 1]
                            # The ON condition might be a parenthesis group or a direct comparison
                            # We might need to check if `on_condition_token` is grouped
                            # For simplicity, we can walk the sub-tokens of the ON condition
                            if on_condition_token.is_group:
                                for oc_sub in on_condition_token.tokens:
                                    if isinstance(oc_sub, Comparison):
                                        join_columns.extend(extract_columns_from_comparison(oc_sub))
                            else:
                                # If it's not a group, it might be a single comparison
                                # We can attempt to parse if it’s a Comparison object
                                if isinstance(on_condition_token, Comparison):
                                    join_columns.extend(extract_columns_from_comparison(on_condition_token))
    
    # If we haven't found any FROM or JOIN references in the group logic above, we can do a fallback:
    # Some queries might present FROM table directly at the top level.
    # We'll do a second pass to extract them, scanning top-level tokens directly.
    # (This is a naive approach; more robust logic would unify the approach).
    if not tables:
        for i, token in enumerate(stmt.tokens):
            if token.match(Keyword, ["FROM", "JOIN"]):
                if i+1 < len(stmt.tokens):
                    next_token = stmt.tokens[i+1]
                    tables.extend(extract_tables(next_token))

    # 2. Parse the WHERE clause to find columns
    if where_clause:
        # The Where object has sub-tokens which may include comparisons
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                # Extract columns from the comparison
                where_columns.extend(extract_columns_from_comparison(token))
            elif token.is_group:
                # Possibly a parenthesis or nested condition
                for subtoken in token.tokens:
                    if isinstance(subtoken, Comparison):
                        where_columns.extend(extract_columns_from_comparison(subtoken))

    # Build comma-separated strings
    tables_str       = ",".join(tables)
    join_cols_str    = ",".join(join_columns)
    where_cols_str   = ",".join(where_columns)

    return (tables_str, query_str, join_cols_str, where_cols_str)

# Register UDF
parse_sql_udf = udf(
    parse_sql_with_sqlparse,
    StructType([
        StructField("tables_in_from_join", StringType(), True),
        StructField("original_query",       StringType(), True),
        StructField("join_columns",         StringType(), True),
        StructField("where_columns",        StringType(), True),
    ])
)

parsed_df = df.withColumn("parsed", parse_sql_udf(col("query")))

final_df = parsed_df.select(
    col("parsed.tables_in_from_join").alias("tables_in_from_join"),
    col("parsed.original_query").alias("actual_query"),
    col("parsed.join_columns").alias("columns_for_joins"),
    col("parsed.where_columns").alias("columns_in_where")
)

final_df.show(truncate=False)
