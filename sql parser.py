import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Whitespace

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType





spark = SparkSession.builder.getOrCreate()

# Sample input DataFrame with a column named "query"
data = [
    ("SELECT a.col1, b.col2 FROM tableA a JOIN tableB b ON a.id = b.id WHERE a.col3 = 'xyz'",),
    ("SELECT x, y FROM schema1.tableC WHERE z = 100",),
    ("SELECT * FROM db.tableD d JOIN tableE e ON d.key = e.key AND d.category = e.category",),
    ("SELECT * FROM tableF f JOIN (SELECT * FROM tableG) g ON f.g_id = g.g_id",),
]
df = spark.createDataFrame(data, ["query"])

def extract_tables(token):
    """
    Extract real table names (without alias) from a token that could be:
      - An Identifier (e.g. "tableA a")
      - An IdentifierList (e.g. "tableA a, tableB b")
    Returns a list of strings (the actual table names).
    """
    extracted = []
    if isinstance(token, Identifier):
        real_name = token.get_real_name()  # e.g. "tableA"
        if real_name:
            extracted.append(real_name)
        else:
            # Fallback: If get_real_name() is None, maybe no alias or weird syntax
            extracted.append(token.value)
    elif isinstance(token, IdentifierList):
        for sub_id in token.get_identifiers():
            real_name = sub_id.get_real_name()
            if real_name:
                extracted.append(real_name)
            else:
                extracted.append(sub_id.value)
    return extracted

def extract_columns_from_comparison(comp_token):
    """
    Extract column references from a sqlparse.sql.Comparison token (e.g. 'a.id = b.id').
    Returns a list of strings (left-side and right-side).
    """
    comp_cols = []
    if isinstance(comp_token, Comparison):
        left = comp_token.left.value.strip()
        right = comp_token.right.value.strip()
        comp_cols.extend([left, right])
    return comp_cols

def parse_sql_with_sqlparse(query_str: str):
    """
    Parses a single SQL statement using sqlparse to extract:
      - tables in FROM or JOIN (only real table names, no aliases)
      - original query
      - columns used in join (ON clauses)
      - columns in WHERE clause

    Returns a tuple of 4 strings:
      (tables_in_from_join, original_query, join_columns, where_columns)
    """
    if not query_str:
        return ("", "", "", "")

    parsed = sqlparse.parse(query_str)
    if not parsed:
        return ("", query_str, "", "")

    stmt = parsed[0]  # Assume a single statement for simplicity

    tables = set()
    join_columns = []
    where_columns = []

    where_clause = None  # Will store a reference to the WHERE block if found

    # Walk the statement tokens
    for token in stmt.tokens:
        # Skip whitespace
        if token.is_whitespace or token.ttype is Whitespace:
            continue

        # If token is a group (e.g. Parenthesis, Where, IdentifierList)
        if token.is_group:
            # Check if it's a WHERE block
            if isinstance(token, Where):
                where_clause = token
            else:
                # Dive deeper
                for subtoken in token.tokens:
                    # If subtoken is FROM or JOIN keyword
                    if subtoken.match(Keyword, ["FROM", "JOIN"]):
                        # Next token(s) might contain the tables
                        idx = token.tokens.index(subtoken)
                        if idx + 1 < len(token.tokens):
                            table_token = token.tokens[idx + 1]
                            tables.update(extract_tables(table_token))

                    # If subtoken is "ON" => parse join condition
                    if subtoken.match(Keyword, ["ON"]):
                        on_idx = token.tokens.index(subtoken)
                        if on_idx + 1 < len(token.tokens):
                            on_condition = token.tokens[on_idx + 1]
                            # The ON condition might be grouped or direct
                            if on_condition.is_group:
                                for oc_sub in on_condition.tokens:
                                    if isinstance(oc_sub, Comparison):
                                        join_columns.extend(extract_columns_from_comparison(oc_sub))
                            elif isinstance(on_condition, Comparison):
                                join_columns.extend(extract_columns_from_comparison(on_condition))

        # Sometimes FROM or JOIN occur at top level
        if token.match(Keyword, ["FROM", "JOIN"]):
            # Next token might hold table references
            idx = list(stmt.tokens).index(token)
            if idx + 1 < len(stmt.tokens):
                next_token = stmt.tokens[idx + 1]
                tables.update(extract_tables(next_token))

    # If we found a WHERE clause
    if where_clause:
        for wtoken in where_clause.tokens:
            if isinstance(wtoken, Comparison):
                where_columns.extend(extract_columns_from_comparison(wtoken))
            elif wtoken.is_group:
                for subtoken in wtoken.tokens:
                    if isinstance(subtoken, Comparison):
                        where_columns.extend(extract_columns_from_comparison(subtoken))

    # Build comma-separated strings
    tables_str     = ",".join(tables)            # unique table names only
    join_cols_str  = ",".join(join_columns)
    where_cols_str = ",".join(where_columns)

    return (tables_str, query_str, join_cols_str, where_cols_str)

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
