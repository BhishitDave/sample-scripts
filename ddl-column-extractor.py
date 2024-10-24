import re

def extract_columns_from_ddl(ddl_statement):
    """
    Extract column names from a DDL statement and return them in double quotes.
    Handles column names that are:
    - Without quotes
    - In single quotes
    - In double quotes
    - Names with spaces
    """
    # Remove line breaks and extra spaces to simplify processing, but preserve spaces in quoted strings
    ddl_statement = ' '.join(ddl_statement.split())
    
    # Find the part between CREATE TABLE and the first opening parenthesis
    table_pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(\w+)\s*\((.*)\)', re.IGNORECASE)
    
    try:
        # Extract the columns part
        columns_part = table_pattern.search(ddl_statement).group(2)
        
        # Split by comma, but not commas within parentheses (for handling function calls in default values)
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
        
        # Extract column names using regex that handles spaces within quotes
        column_pattern = re.compile(r'^["\']?([\w\d_\s]+)["\']?\s+.*$')
        column_names = []
        
        for column in columns:
            # Handle quoted names with spaces
            quoted_pattern = re.compile(r'^["\'](.*?)["\']\s+.*$')
            unquoted_pattern = re.compile(r'^([\w\d_]+)\s+.*$')
            
            quoted_match = quoted_pattern.match(column.strip())
            unquoted_match = unquoted_pattern.match(column.strip())
            
            if quoted_match:
                column_names.append(f'"{quoted_match.group(1)}"')
            elif unquoted_match:
                column_names.append(f'"{unquoted_match.group(1)}"')
        
        return '\n'.join(column_names)
        
    except AttributeError:
        return "Error: Invalid DDL statement format"

# Example usage
if __name__ == "__main__":
    # Test DDL with different quote styles including spaces in names
    test_ddl = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        "first name" VARCHAR(50),
        'last name' VARCHAR(50),
        "email address" TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "contact phone" INTEGER DEFAULT 0,
        "user status" VARCHAR(20) DEFAULT 'active'
    )
    """
    
    print(extract_columns_from_ddl(test_ddl))
