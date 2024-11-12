import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Case, Parenthesis
from sqlparse.tokens import Keyword, DML

def extract_alias(token):
    """Extract alias from a SQL token"""
    # Handle CASE statements
    if isinstance(token, Case):
        # Find the AS part after the CASE statement
        for tok in token.tokens:
            if tok.ttype == Keyword and tok.value.upper() == 'AS':
                # The next token should be the alias
                next_token = token.tokens[token.tokens.index(tok) + 1]
                return next_token.value.strip('"\'')
        return None
        
    # Handle general expressions with AS
    if hasattr(token, 'tokens'):
        as_index = None
        for i, tok in enumerate(token.tokens):
            if tok.ttype == Keyword and tok.value.upper() == 'AS':
                as_index = i
                break
        if as_index is not None and as_index + 1 < len(token.tokens):
            return token.tokens[as_index + 1].value.strip('"\'')
    return None

def normalize_identifier(identifier):
    """Normalize identifier by removing quotes and converting to lowercase"""
    return identifier.strip('"\'').lower()

def extract_select_columns(select_stmt):
    """Extract column names/aliases from SELECT statement"""
    select_columns = []
    
    def process_token(token):
        """Process a token and extract column information"""
        if isinstance(token, Identifier):
            alias = extract_alias(token)
            if alias:
                return normalize_identifier(alias)
            # If no alias, use the base name
            return normalize_identifier(token.get_name())
        elif isinstance(token, Function):
            alias = extract_alias(token)
            if alias:
                return normalize_identifier(alias)
            # If no alias for function, try to find the last identifier
            for tok in reversed(token.tokens):
                if isinstance(tok, Identifier):
                    return normalize_identifier(tok.get_name())
        elif isinstance(token, Case):
            alias = extract_alias(token)
            if alias:
                return normalize_identifier(alias)
        elif isinstance(token, Parenthesis):
            # Handle subexpressions in parentheses
            alias = extract_alias(token)
            if alias:
                return normalize_identifier(alias)
        return None

    parsed = sqlparse.parse(select_stmt)[0]
    
    # Find the SELECT part
    select_seen = False
    from_seen = False
    
    for token in parsed.tokens:
        # Skip everything before SELECT
        if token.ttype == DML and token.value.upper() == 'SELECT':
            select_seen = True
            continue
            
        if not select_seen:
            continue
            
        # Stop at FROM clause
        if token.ttype == Keyword and token.value.upper() == 'FROM':
            from_seen = True
            break
            
        # Process column list
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                col_name = process_token(identifier)
                if col_name:
                    select_columns.append(col_name)
        elif isinstance(token, (Identifier, Function, Case, Parenthesis)):
            col_name = process_token(token)
            if col_name:
                select_columns.append(col_name)
                
    return select_columns

def extract_insert_columns(insert_stmt):
    """Extract column names from INSERT statement"""
    insert_columns = []
    
    parsed = sqlparse.parse(insert_stmt)[0]
    
    # Find the columns part between parentheses after INSERT INTO table_name
    in_columns = False
    paren_count = 0
    
    for token in parsed.tokens:
        if isinstance(token, Parenthesis) and not in_columns:
            in_columns = True
            # Extract identifiers from the parenthesis content
            content = token.value.strip('()')
            # Split by comma and clean up each column name
            columns = [normalize_identifier(col.strip()) for col in content.split(',')]
            insert_columns.extend(columns)
            break
            
    return insert_columns

def verify_sql_columns(sql):
    """Verify that INSERT and SELECT columns match"""
    # Split the SQL into insert and select parts
    parts = sql.split('SELECT', 1)
    if len(parts) != 2:
        return "Error: Could not split SQL into INSERT and SELECT parts"
        
    insert_part = parts[0]
    select_part = 'SELECT' + parts[1]
    
    # Extract columns
    insert_columns = extract_insert_columns(insert_part)
    select_columns = extract_select_columns(select_part)
    
    # Compare lengths first
    if len(insert_columns) != len(select_columns):
        return {
            'match': False,
            'insert_columns': insert_columns,
            'select_columns': select_columns,
            'error': 'Number of columns does not match'
        }
    
    # Compare columns
    mismatches = []
    for i, (ins_col, sel_col) in enumerate(zip(insert_columns, select_columns)):
        if ins_col != sel_col:
            mismatches.append(f"Position {i+1}: INSERT '{ins_col}' ≠ SELECT '{sel_col}'")
    
    return {
        'match': len(mismatches) == 0,
        'mismatches': mismatches if mismatches else None,
        'insert_columns': insert_columns,
        'select_columns': select_columns
    }

# Example usage
if __name__ == "__main__":
    # Test SQL with various cases
    test_sql = """
    INSERT INTO target_table (
        id,
        first_name,
        last_name,
        full_name,
        age,
        status,
        modified_date
    )
    SELECT 
        user_id as id,
        fname as first_name,
        lname as last_name,
        CONCAT(fname, ' ', lname) as full_name,
        CASE 
            WHEN birth_date IS NULL THEN 0 
            ELSE DATEDIFF(year, birth_date, CURRENT_DATE)
        END as age,
        CASE status
            WHEN 1 THEN 'active'
            WHEN 0 THEN 'inactive'
            ELSE 'unknown'
        END as status,
        CURRENT_TIMESTAMP as modified_date
    FROM source_table;
    """
    
    result = verify_sql_columns(test_sql)
    
    if isinstance(result, str):
        print(result)
    else:
        if result['match']:
            print("Column match: True")
            print("\nMatched columns:")
            for i, (ins, sel) in enumerate(zip(result['insert_columns'], result['select_columns'])):
                print(f"{i+1}. {ins} = {sel}")
        else:
            print("Column match: False")
            print("\nMismatches found:")
            for mismatch in result['mismatches']:
                print(mismatch)
