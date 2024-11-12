import re
from typing import Tuple, List, Dict

class SQLValidator:
    def __init__(self):
        # Common SQL functions that might appear in SELECT
        self.sql_functions = [
            'SUBSTRING', 'SUBSTR', 'TRIM', 'UPPER', 'LOWER',
            'COALESCE', 'NVL', 'CASE', 'CAST', 'CONVERT',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'
        ]
    
    def extract_insert_columns(self, sql: str) -> List[str]:
        """Extract column names from INSERT INTO statement."""
        try:
            # Match everything between first ( and ) after INSERT INTO table_name
            match = re.search(r'INSERT\s+INTO\s+\w+\s*\((.*?)\)', sql, re.IGNORECASE)
            if not match:
                raise ValueError("No columns found in INSERT statement")
            
            # Split by comma and clean up whitespace
            columns = [col.strip() for col in match.group(1).split(',')]
            return columns
        except Exception as e:
            raise ValueError(f"Error parsing INSERT columns: {str(e)}")

    def extract_select_columns(self, sql: str) -> List[str]:
        """Extract column expressions from SELECT statement."""
        try:
            # Find the SELECT part until FROM
            match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not match:
                raise ValueError("No SELECT statement found")
            
            select_part = match.group(1)
            
            # Handle nested parentheses and complex expressions
            columns = []
            current_column = ''
            paren_count = 0
            
            for char in select_part:
                if char == ',' and paren_count == 0:
                    if current_column.strip():
                        columns.append(current_column.strip())
                    current_column = ''
                else:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    current_column += char
            
            if current_column.strip():
                columns.append(current_column.strip())
            
            # Extract aliases or last part of expression
            final_columns = []
            for col in columns:
                # Check for AS keyword
                as_match = re.search(r'\s+AS\s+(\w+)$', col, re.IGNORECASE)
                if as_match:
                    final_columns.append(as_match.group(1))
                else:
                    # If no AS, take the last part after space or dot
                    parts = col.strip().split()
                    if len(parts) > 1:
                        final_columns.append(parts[-1])
                    else:
                        final_columns.append(col.strip())
            
            return final_columns
        except Exception as e:
            raise ValueError(f"Error parsing SELECT columns: {str(e)}")

    def validate_columns(self, sql: str) -> Tuple[bool, Dict[str, List[str]]]:
        """
        Validate if INSERT and SELECT columns match.
        Returns (is_valid, mismatch_dict)
        """
        try:
            insert_cols = self.extract_insert_columns(sql)
            select_cols = self.extract_select_columns(sql)
            
            # Compare lengths first
            if len(insert_cols) != len(select_cols):
                return False, {
                    "insert_columns": insert_cols,
                    "select_columns": select_cols,
                    "error": "Number of columns doesn't match"
                }
            
            # Compare each column
            mismatches = {
                "insert_columns": [],
                "select_columns": []
            }
            
            for i, (insert_col, select_col) in enumerate(zip(insert_cols, select_cols)):
                if insert_col.lower() != select_col.lower():
                    mismatches["insert_columns"].append(f"{i+1}. {insert_col}")
                    mismatches["select_columns"].append(f"{i+1}. {select_col}")
            
            return len(mismatches["insert_columns"]) == 0, mismatches
            
        except Exception as e:
            return False, {"error": str(e)}

# Example usage
def validate_sql_statement(sql: str) -> None:
    validator = SQLValidator()
    is_valid, results = validator.validate_columns(sql)
    
    if is_valid:
        print("✅ Columns match perfectly!")
    else:
        if "error" in results:
            print(f"❌ Error: {results['error']}")
        else:
            print("❌ Column mismatch found:")
            print("\nInsert columns with issues:")
            for col in results["insert_columns"]:
                print(f"  - {col}")
            print("\nCorresponding Select columns:")
            for col in results["select_columns"]:
                print(f"  - {col}")



# Example SQL with matching columns
sql1 = """
INSERT INTO users (id, full_name, age, status)
SELECT 
    user_id AS id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    CASE 
        WHEN birth_date IS NULL THEN 0 
        ELSE DATEDIFF(YEAR, birth_date, GETDATE()) 
    END AS age,
    active_status AS status
FROM source_table
"""

# Example SQL with mismatched columns
sql2 = """
INSERT INTO users (id, full_name, age, status)
SELECT 
    user_id AS id,
    CONCAT(first_name, ' ', last_name) AS name,
    CASE 
        WHEN birth_date IS NULL THEN 0 
        ELSE DATEDIFF(YEAR, birth_date, GETDATE()) 
    END AS user_age,
    active_status AS user_status
FROM source_table
"""

validate_sql_statement(sql1)  # Will show match
validate_sql_statement(sql2)  