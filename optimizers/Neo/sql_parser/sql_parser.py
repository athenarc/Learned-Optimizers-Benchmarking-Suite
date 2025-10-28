# sql_parser.py

import sqlparse
import re
from sqlparse.sql import Identifier, IdentifierList, Function, Where, Parenthesis, Case
from sqlparse.tokens import DML, Keyword, Punctuation, Text, Comparison, String, Comment, Name, Operator
from typing import List, Set, Dict, Tuple, Any
from .config import DB_CONFIG, get_database_schema
import psycopg2

def is_subselect(parsed):
    """Check if the parsed token is a subquery."""
    if not parsed.is_group:
        return False
    return any(item.ttype is DML and item.value.upper() == 'SELECT' for item in parsed.tokens)

def extract_from_part(parsed):
    """Extract table names from the FROM clause."""
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'HAVING', 'LIMIT']:
                from_seen = False
            elif isinstance(item, Identifier):
                yield item.get_real_name()
            elif isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_real_name()
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_select_part(parsed):
    """Extract column names from the SELECT clause."""
    select_seen = False
    sql_functions = {'MIN', 'MAX', 'COUNT', 'SUM', 'AVG'}
    for token in parsed.tokens:
        if select_seen:
            if token.ttype is Punctuation and token.value == ',':
                continue
            if token.ttype is Keyword and token.value.upper() in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']:
                break
            if isinstance(token, Identifier):
                yield from extract_identifier(token, sql_functions)
            elif isinstance(token, Function):
                yield token.get_real_name()
            elif isinstance(token, IdentifierList):
                for t in token.get_identifiers():
                    yield from extract_identifier(t, sql_functions)
        elif token.ttype is DML and token.value.upper() == 'SELECT':
            select_seen = True

def extract_identifier(token, sql_functions):
    """Extract column names from an Identifier token."""
    if '(' in token.value:
        for func in sql_functions:
            if token.value.upper().startswith(func):
                column_name = token.value.split('(')[1].split(')')[0]
                # Remove new lines and tabs
                column_name = re.sub(r'\s+', ' ', column_name)
                yield column_name
    else:
        yield token.get_real_name()

def extract_where_part(parsed):
    """Extract predicates from the WHERE clause."""
    predicates = []
    for item in parsed.tokens:
        if isinstance(item, Where):
            where_tokens = list(item.flatten())
            predicates.extend(process_where_tokens(where_tokens))
    return predicates

def process_where_tokens(tokens):
    """Process tokens in the WHERE clause to extract predicates."""
    predicates = []
    predicate = []
    i = 1

    def skip_whitespace():
        nonlocal i
        while i < len(tokens) and tokens[i].ttype in (Text.Whitespace, Text.Whitespace.Newline):
            i += 1

    def add_predicate():
        if predicate:
            predicates.append("".join(predicate))
            predicate.clear()

    def handle_keyword(keyword):
        nonlocal i
        # print(f"Handling {keyword}")
        if keyword in ['AND', 'OR']:
            # Add the current predicate to the list and start a new one
            add_predicate()
        else:
            predicate.append(f" {tokens[i].value.upper()} ")  # Add keyword
        i += 1
        skip_whitespace()

    def handle_comparison(operator):
        nonlocal i
        # print(f"Handling {operator}")
        predicate.append(f" {tokens[i].value} ")  # Add comparison operator (e.g., "=", "!=", ">", "<")
        i += 1
    
    def handle_punctuation():
        nonlocal i
        # print("Handling Punctuation")
        predicate.append(tokens[i].value)
        i += 1
    
    def handle_date_literal():
        nonlocal i
        # print("Handling DATE")
        # Handle DATE 'YYYY-MM-DD' format
        date_parts = [tokens[i].value]  # Start with 'date'
        i += 1
        
        # Skip any whitespace between 'date' and the date string
        skip_whitespace()
        
        # Add the date string (should be a String.Single token)
        if i < len(tokens) and tokens[i].ttype is String.Single:
            date_parts.append(tokens[i].value)
            i += 1
        else:
            # If we don't find the expected date string, just use what we have
            pass
        
        # Combine the parts with a single space
        date_value = ' '.join(date_parts)
        predicate.append(date_value)
        skip_whitespace()

    def handle_interval():
        nonlocal i
        # print("Handling INTERVAL")
        # Handle INTERVAL 'value' unit
        interval_parts = []
        while i < len(tokens):
            if tokens[i].value.upper() == 'INTERVAL':
                interval_parts.append(tokens[i].value)
                i += 1
                skip_whitespace()
                # Get the interval value (string literal)
                if i < len(tokens) and tokens[i].ttype is String.Single:
                    interval_parts.append(tokens[i].value)
                    i += 1
                    skip_whitespace()
                    # Get the interval unit (year, month, day, etc.)
                    if i < len(tokens) and tokens[i].ttype is Keyword:
                        interval_parts.append(tokens[i].value)
                        i += 1
                        break
            else:
                break
        predicate.append(" ".join(interval_parts))

    def handle_arithmetic():
        nonlocal i
        # print("Handling Arithmetic")
        # Handle + or - operators
        operator = tokens[i].value
        i += 1
        skip_whitespace()
        if i < len(tokens):
            if tokens[i].value.upper() == 'INTERVAL':
                handle_interval()
            else:
                predicate.append(operator + tokens[i].value)
                i += 1

    def handle_parenthesis():
        nonlocal i
        # Start of the parenthesized expression
        parenthesis_start = i
        parenthesis_depth = 0
        parenthesized_content = []
        
        # First, find the matching closing parenthesis
        while i < len(tokens):
            if tokens[i].value == '(':
                parenthesis_depth += 1
            elif tokens[i].value == ')':
                parenthesis_depth -= 1
                if parenthesis_depth == 0:
                    break
            i += 1
        
        # Now collect all tokens between the parentheses (including nested ones)
        i = parenthesis_start + 1  # Skip the opening parenthesis
        parenthesis_depth = 1  # We're inside one level of parentheses
        
        while i < len(tokens) and parenthesis_depth > 0:
            if tokens[i].value == '(':
                parenthesis_depth += 1
            elif tokens[i].value == ')':
                parenthesis_depth -= 1
                if parenthesis_depth == 0:
                    break
            
            # Skip newline tokens but keep everything else
            if tokens[i].ttype not in (Text.Whitespace.Newline):
                parenthesized_content.append(tokens[i].value)
            i += 1
        
        # Join the content to form a single predicate
        parenthesized_predicate = '(' + ''.join(parenthesized_content) + ')'
        predicate.append(parenthesized_predicate)
        
        # Move past the closing parenthesis
        if i < len(tokens) and tokens[i].value == ')':
            i += 1
                
    def handle_sets():
        nonlocal i
        # print("Handling SETS")
        predicate.append(f" {tokens[i].value.upper()} ")  # Add "IN"
        i += 1
        skip_whitespace()
        handle_parenthesis()

    def handle_between():
        nonlocal i
        # print("Handling BETWEEN")
        predicate.append(f" {tokens[i].value.upper()} ")  # Add "BETWEEN"
        i += 1
        skip_whitespace()
        if i < len(tokens):
            predicate.append(tokens[i].value)  # Add value1
            i += 1
        skip_whitespace()
        if i < len(tokens) and tokens[i].value.upper() == 'AND':
            predicate.append(f" {tokens[i].value.upper()} ")  # Add "AND"
            i += 1
            skip_whitespace()
            if i < len(tokens):
                predicate.append(tokens[i].value)  # Add value2
                i += 1

    while i < len(tokens):
        token = tokens[i]
        # print(f"Token: {token}")
        # Skip whitespace and newline tokens
        if token.ttype in (Text.Whitespace, Text.Whitespace.Newline):
            i += 1
            continue

        # Break if we encounter FROM or WHERE keywords
        if token.ttype is Keyword and token.value.upper() in ['FROM', 'WHERE']:
            break

        # Handle DATE literals
        if token.value.upper() == 'DATE':
            handle_date_literal()
            continue
            
        # Handle INTERVAL expressions
        if token.value.upper() == 'INTERVAL':
            handle_interval()
            continue
            
        # Handle arithmetic operators (for date + interval)
        if token.ttype is Operator and token.value in ('+', '-'):
            handle_arithmetic()
            continue

        # Handle keywords (NOT, IS, BETWEEN, AND, OR, IN, LIKE)
        if token.ttype is Keyword:
            if token.value.upper() in ['NOT', 'IS', 'NOT NULL', 'NULL', 'AND', 'OR', 'NOT LIKE']:
                handle_keyword(token.value.upper())
            elif token.value.upper() in ['IN','EXISTS']:
                handle_sets()
            elif token.value.upper() == 'BETWEEN':
                handle_between()
            continue

        # Handle comparison operators (=, !=, >, <, >=, <=)
        elif token.ttype is Comparison:
            handle_comparison(token.value.upper())
            continue

        elif token.ttype is Punctuation:
            if token.value.upper() == '(':
                handle_parenthesis()
            elif token.value.upper() == ';':
                i += 1
            else:
                handle_punctuation()
            continue

        elif isinstance(token, Case):
            # Handle CASE expressions in WHERE clause
            case_text = token.value
            predicate.append(case_text)
            i += 1
            continue

        # Handle all other tokens
        # print(f"Handling other token: {token}")
        predicate.append(token.value)
        i += 1

    # Add the last predicate if it exists
    add_predicate()

    return predicates

def extract_table_aliases(parsed):
    """Extract table aliases and their corresponding full names from the FROM clause."""
    aliases = {}
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                aliases.update(extract_table_aliases(item))
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'HAVING', 'LIMIT', 'WHERE']:
                from_seen = False
            elif isinstance(item, Identifier):
                handle_identifier(item, aliases)
            elif isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    handle_identifier(identifier, aliases)
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
    return aliases

def handle_identifier(identifier, aliases):
    """Handle Identifier tokens to extract table aliases."""
    # Get the raw value of the identifier
    raw_value = identifier.value.strip()
    
    # Handle cases with explicit AS keyword
    if " AS " in raw_value.upper():
        parts = re.split(r'\s+AS\s+', raw_value, flags=re.IGNORECASE)
        if len(parts) == 2:
            table_name = parts[0].strip()
            alias = parts[1].strip()
            aliases[alias] = table_name
        return
    
    # Handle cases without AS keyword (implicit aliases)
    # Split on whitespace but be careful with schema.table names
    parts = re.split(r'\s+', raw_value)
    
    if len(parts) == 2:
        # Simple case: "table alias" or "schema.table alias"
        table_name = parts[0].strip()
        alias = parts[1].strip()
        aliases[alias] = table_name
    elif len(parts) == 1:
        # No alias, just table name
        table_name = identifier.get_real_name()
        aliases[table_name] = table_name
    else:
        # Handle more complex cases like "schema.table alias" with multiple dots
        # Try to find the last whitespace that separates table from alias
        # This handles cases like "db.schema.table alias"
        last_space_pos = raw_value.rfind(' ')
        if last_space_pos > 0:
            table_name = raw_value[:last_space_pos].strip()
            alias = raw_value[last_space_pos+1:].strip()
            aliases[alias] = table_name
        else:
            # Fallback - just use the real name
            table_name = identifier.get_real_name()
            aliases[table_name] = table_name

def replace_aliases_with_full_names(columns, predicates, aliases):
    """Replace aliases with full table names in columns and predicates."""
    updated_columns = [replace_alias(column, aliases) for column in columns]
    updated_predicates = [replace_alias(predicate, aliases) for predicate in predicates]
    return updated_columns, updated_predicates

def replace_alias(text, aliases):
    """Replace alias with full table name in a given text."""
    for alias, table_name in aliases.items():
        text = re.sub(rf'\b{alias}\.', f'{table_name}.', text)
    return text

def separate_joins_and_filters(predicates, aliases):
    """Separate join conditions from filter conditions in the WHERE clause."""
    joins = set()
    filters = set()
    for predicate in predicates:
        if '=' in predicate and '.' in predicate:
            left, right = predicate.split('=', 1)
            if '.' in left.strip() and '.' in right.strip():
                joins.add(predicate)
                continue
        filters.add(predicate)
    return {'joins': joins, 'filters': filters}

def parse_sql(query):
    parsed = sqlparse.parse(query)[0]
    columns = list(extract_select_part(parsed))
    tables = list(extract_from_part(parsed))
    predicates = extract_where_part(parsed)
    aliases = extract_table_aliases(parsed)
    # columns, predicates = replace_aliases_with_full_names(columns, predicates, aliases)

    try:
        DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        # Establish database connection
        conn = psycopg2.connect(DB_URL)
        db_schema = get_database_schema(conn)
        if db_schema is None:
            raise Exception("Failed to fetch database schema")        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

    # Create ordered lists for joins and filters
    ordered_joins = []
    ordered_filters = []

   # Build column to tables mapping if schema is available
    column_to_tables = {}
    if db_schema:
        for table, cols in db_schema.items():
            for col in cols:
                if col not in column_to_tables:
                    column_to_tables[col] = []
                column_to_tables[col].append(table)
    
    def is_literal(value):
        value = value.strip()
        return (value.replace('.', '').isdigit() or  # Number
                (value.startswith("'") and value.endswith("'")) or  # String
                value.upper() in ('NULL', 'TRUE', 'FALSE'))  # Special values
    
    def get_column_tables(col_ref, query_tables):
        """Resolve a column reference to its table(s)"""
        
        if '.' in col_ref:  # Qualified reference
            qualifier, col = col_ref.split('.', 1)
            if qualifier in aliases:  # Alias reference
                return [qualifier]
            elif qualifier in query_tables:  # Table reference
                return [qualifier]
            else:  # Possibly schema.table
                for table in query_tables:
                    if table.endswith(f".{qualifier}"):
                        return [table]
        elif col_ref in column_to_tables:  # Unqualified reference
            # Return tables that exist in this query
            return [t for t in column_to_tables[col_ref] if t in query_tables]
        return []
    
    for predicate in predicates:
        if '=' in predicate:
            left, right = [part.strip() for part in predicate.split('=', 1)]
            # Skip if either side is a literal
            if is_literal(left) or is_literal(right):
                ordered_filters.append(predicate)
                continue
                
            # Get tables for each side
            left_tables = get_column_tables(left, tables)
            right_tables = get_column_tables(right, tables)
            # Determine if this is a join (columns from different tables)
            if (left_tables and right_tables and 
                not set(left_tables).intersection(right_tables)):
                ordered_joins.append(predicate)
            else:
                ordered_filters.append(predicate)
        else:
            ordered_filters.append(predicate)

    return {
        'select_columns': set(columns),
        'from_tables': set(tables),
        'aliases': aliases,
        'predicates': set(predicates),
        'joins': ordered_joins,
        'filters': ordered_filters
    }