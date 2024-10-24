import re

def extract_table_name(ddl_statement):
    """Extract table name from DDL statement"""
    table_pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(["\']?[\w\s]+["\']?)\s*\(', re.IGNORECASE)
    match = table_pattern.search(ddl_statement)
    if match:
        table_name = match.group(1).strip('"\'')
        return table_name
    return None

def extract_columns_from_ddl(ddl_statement):
    """
    Extract column names from a DDL statement and return them with their original quotes.
    Returns a list of tuples: (original_name, normalized_name)
    """
    # Remove line breaks and extra spaces to simplify processing, but preserve spaces in quoted strings
    ddl_statement = ' '.join(ddl_statement.split())
    
    # Find the part between CREATE TABLE and the first opening parenthesis
    table_pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(?:["\']?[\w\s]+["\']?)\s*\((.*)\)', re.IGNORECASE)
    
    try:
        # Extract the columns part
        columns_part = table_pattern.search(ddl_statement).group(1)
        
        # Split by comma, but not commas within parentheses
        columns = []
        paren_count = 0
        current_column = ''
        in_quotes = False
        quote_char = None
        
        for char in columns_part:
            # Handle quotes
            if char in ['"', "'"] and not current_column.endswith('\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            
            # Handle parentheses
            if char == '(' and not in_quotes and not current_column.endswith('\\'):
                paren_count += 1
            elif char == ')' and not in_quotes and not current_column.endswith('\\'):
                paren_count -= 1
            
            # Split columns
            if char == ',' and paren_count == 0 and not in_quotes:
                columns.append(current_column.strip())
                current_column = ''
            else:
                current_column += char
                
        # Don't forget the last column
        if current_column.strip():
            columns.append(current_column.strip())
        
        # Extract column names
        column_info = []
        for column in columns:
            # Handle quoted names with spaces
            quoted_pattern = re.compile(r'^["\'](.*?)["\']\s+.*$')
            unquoted_pattern = re.compile(r'^([\w\d_]+)\s+.*$')
            
            quoted_match = quoted_pattern.match(column.strip())
            unquoted_match = unquoted_pattern.match(column.strip())
            
            if quoted_match:
                original_name = quoted_match.group(1)
                normalized_name = original_name.replace(' ', '_')
                column_info.append((f'"{original_name}"', normalized_name))
            elif unquoted_match:
                name = unquoted_match.group(1)
                column_info.append((f'"{name}"', name))
        
        return column_info
        
    except AttributeError:
        return []

def generate_insert_statement(source_ddl, target_ddl):
    """Generate Redshift INSERT INTO statement based on source and target DDLs"""
    source_table = extract_table_name(source_ddl)
    target_table = extract_table_name(target_ddl)
    
    if not source_table or not target_table:
        return "Error: Could not extract table names"
    
    source_columns = extract_columns_from_ddl(source_ddl)
    target_columns = extract_columns_from_ddl(target_ddl)
    
    if not source_columns or not target_columns:
        return "Error: Could not extract columns"
    
    # Create the INSERT INTO statement
    target_cols = [col[0] for col in target_columns]
    source_cols = []
    
    # Generate SELECT expressions with AS clauses for columns with spaces
    for col in source_columns:
        original_name = col[0]
        normalized_name = col[1]
        if ' ' in original_name.strip('"'):
            source_cols.append(f'{original_name} as {normalized_name}')
        else:
            source_cols.append(original_name)
    
    insert_stmt = f"INSERT INTO {target_table} (\n    "
    insert_stmt += ",\n    ".join(target_cols)
    insert_stmt += "\n)\nSELECT\n    "
    insert_stmt += ",\n    ".join(source_cols)
    insert_stmt += f"\nFROM {source_table};"
    
    return insert_stmt

# Example usage
if __name__ == "__main__":
    # Test DDLs
    source_ddl = """
    CREATE TABLE source_employees (
        id INTEGER PRIMARY KEY,
        "first name" VARCHAR(50),
        'last name' VARCHAR(50),
        "email address" TEXT NOT NULL,
        created_at TIMESTAMP,
        "phone number" VARCHAR(20)
    )
    """
    
    target_ddl = """
    CREATE TABLE target_employees (
        id INTEGER PRIMARY KEY,
        "first name" VARCHAR(50),
        "last name" VARCHAR(50),
        "email address" TEXT NOT NULL,
        created_at TIMESTAMP,
        "phone number" VARCHAR(20)
    )
    """
    
    result = generate_insert_statement(source_ddl, target_ddl)
    print(result)
