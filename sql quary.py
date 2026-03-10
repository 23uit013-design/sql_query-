"""
Natural Language to SQL Query Generator - OPTIMIZED VERSION
Loads faster with caching and optimized database access
"""

import sqlite3
import re
from flask import Flask, render_template_string, request, jsonify
from typing import Dict, List, Tuple, Optional
import os
import webbrowser
import threading
import time
from functools import lru_cache

# ==================== FLASK APP INITIALIZATION ====================
app = Flask(__name__)

# Disable debug mode for faster loading
app.debug = False

# ==================== CACHED DATABASE SCHEMA ====================
# Global cache for schema to avoid repeated database queries
_SCHEMA_CACHE = None
_DB_PATH = 'database.db'

# ==================== DATABASE SETUP ====================
def setup_database():
    """
    Creates and populates the SQLite database with sample tables
    Only runs once when database doesn't exist
    """
    
    # Only create database if it doesn't exist
    if os.path.exists(_DB_PATH):
        print("✅ Database already exists")
        return
    
    print("🔄 Creating new database...")
    
    # Connect to SQLite database
    conn = sqlite3.connect(_DB_PATH)
    cursor = conn.cursor()
    
    # Create tables (with IF NOT EXISTS for safety)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            city TEXT,
            state TEXT,
            country TEXT,
            phone TEXT,
            registration_date DATE
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            category TEXT,
            price DECIMAL(10, 2),
            stock_quantity INTEGER,
            description TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            order_date DATE,
            total_amount DECIMAL(10, 2),
            status TEXT,
            FOREIGN KEY (customer_id) REFERENCES Customers (customer_id)
        )
    ''')
    
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM Customers")
    if cursor.fetchone()[0] == 0:
        # Insert sample data only if tables are empty
        sample_customers = [
            ('John', 'Doe', 'john.doe@email.com', 'Los Angeles', 'California', 'USA', '555-0101', '2023-01-15'),
            ('Jane', 'Smith', 'jane.smith@email.com', 'San Francisco', 'California', 'USA', '555-0102', '2023-02-20'),
            ('Bob', 'Johnson', 'bob.johnson@email.com', 'New York', 'New York', 'USA', '555-0103', '2023-03-10'),
            ('Alice', 'Brown', 'alice.brown@email.com', 'Austin', 'Texas', 'USA', '555-0104', '2023-04-05'),
            ('Charlie', 'Wilson', 'charlie.w@email.com', 'Miami', 'Florida', 'USA', '555-0105', '2023-05-12'),
            ('Diana', 'Martinez', 'diana.m@email.com', 'San Diego', 'California', 'USA', '555-0106', '2023-06-18')
        ]
        
        cursor.executemany('''
            INSERT INTO Customers (first_name, last_name, email, city, state, country, phone, registration_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', sample_customers)
        
        sample_products = [
            ('Laptop Pro', 'Electronics', 1299.99, 50, 'High-performance laptop'),
            ('Smartphone X', 'Electronics', 899.99, 100, 'Latest smartphone model'),
            ('Desk Chair', 'Furniture', 249.99, 30, 'Ergonomic office chair'),
            ('Coffee Maker', 'Appliances', 79.99, 75, 'Automatic coffee machine'),
            ('Running Shoes', 'Sports', 129.99, 200, 'Comfortable running shoes'),
            ('Backpack', 'Accessories', 49.99, 150, 'Waterproof backpack'),
            ('Tablet', 'Electronics', 499.99, 60, '10-inch tablet with stylus'),
            ('Monitor', 'Electronics', 329.99, 40, '27-inch 4K monitor')
        ]
        
        cursor.executemany('''
            INSERT INTO Products (product_name, category, price, stock_quantity, description)
            VALUES (?, ?, ?, ?, ?)
        ''', sample_products)
        
        sample_orders = [
            (1, '2024-01-10', 2199.98, 'Delivered'),
            (2, '2024-01-15', 899.99, 'Delivered'),
            (3, '2024-01-20', 329.98, 'Shipped'),
            (1, '2024-02-01', 79.99, 'Delivered'),
            (4, '2024-02-05', 1299.99, 'Processing'),
            (2, '2024-02-10', 549.98, 'Shipped'),
            (5, '2024-02-15', 379.98, 'Delivered'),
            (6, '2024-02-20', 129.99, 'Pending')
        ]
        
        cursor.executemany('''
            INSERT INTO Orders (customer_id, order_date, total_amount, status)
            VALUES (?, ?, ?, ?)
        ''', sample_orders)
        
        print("✅ Sample data inserted")
    
    conn.commit()
    conn.close()
    print("✅ Database setup completed!")

# ==================== CACHED SCHEMA LOADER ====================
@lru_cache(maxsize=1)
def get_cached_schema():
    """
    Cache the database schema to avoid repeated database queries
    LRU cache ensures schema is only loaded once
    """
    schema = {}
    
    try:
        conn = sqlite3.connect(_DB_PATH)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            # Get column information for each table
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema[table_name] = [
                {
                    'name': col[1],
                    'type': col[2],
                    'primary_key': col[5] == 1
                }
                for col in columns
            ]
        
        conn.close()
        return schema
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return {}

# ==================== SQL GENERATOR CLASS ====================
class SQLGenerator:
    """
    Main class for converting natural language to SQL queries
    Uses cached schema for faster initialization
    """
    
    def __init__(self):
        """Initialize with cached schema - much faster!"""
        self.schema_info = get_cached_schema()
        
    def _get_columns_for_table(self, table_name: str) -> List[str]:
        """Get all column names for a specific table"""
        if table_name in self.schema_info:
            return [col['name'] for col in self.schema_info[table_name]]
        return []
    
    def _extract_keywords(self, question: str) -> Dict:
        """
        Extract relevant keywords from the question
        Optimized regex patterns for faster matching
        """
        question_lower = question.lower()
        
        # Initialize extracted info
        extracted = {
            'action': 'SELECT',
            'tables': [],
            'columns': [],
            'conditions': [],
            'aggregates': [],
            'order_by': None,
            'limit': None,
            'requires_join': False,
            'join_tables': []
        }
        
        # Quick aggregate detection (compiled patterns would be faster, but keeping simple)
        if 'count' in question_lower or 'how many' in question_lower:
            extracted['aggregates'].append('COUNT')
        if 'sum' in question_lower or 'total' in question_lower:
            extracted['aggregates'].append('SUM')
        if 'average' in question_lower or 'mean' in question_lower:
            extracted['aggregates'].append('AVG')
        if 'maximum' in question_lower or 'most' in question_lower or 'highest' in question_lower:
            extracted['aggregates'].append('MAX')
        if 'minimum' in question_lower or 'least' in question_lower or 'lowest' in question_lower:
            extracted['aggregates'].append('MIN')
        
        # Quick ORDER BY detection
        if 'sort by' in question_lower or 'order by' in question_lower:
            extracted['order_by'] = 'ASC'
            if 'descending' in question_lower or 'highest' in question_lower or 'largest' in question_lower:
                extracted['order_by'] = 'DESC'
        
        # Quick LIMIT detection
        if 'top ' in question_lower or 'first ' in question_lower or 'limit ' in question_lower:
            for word in question_lower.split():
                if word.isdigit():
                    extracted['limit'] = int(word)
                    break
        
        # Detect tables (simplified for speed)
        if 'customer' in question_lower or 'client' in question_lower:
            extracted['tables'].append('Customers')
        if 'order' in question_lower or 'purchase' in question_lower:
            extracted['tables'].append('Orders')
        if 'product' in question_lower or 'item' in question_lower:
            extracted['tables'].append('Products')
        
        # Remove duplicates
        extracted['tables'] = list(set(extracted['tables']))
        
        # Check for JOIN requirement
        if len(extracted['tables']) >= 2:
            extracted['requires_join'] = True
            extracted['join_tables'] = extracted['tables']
        elif ('customer' in question_lower and 'order' in question_lower) or \
             ('customer' in question_lower and 'purchase' in question_lower):
            extracted['requires_join'] = True
            extracted['tables'] = ['Customers', 'Orders']
            extracted['join_tables'] = ['Customers', 'Orders']
        
        # Extract conditions (simplified for speed)
        states = ['california', 'new york', 'texas', 'florida']
        for state in states:
            if state in question_lower:
                extracted['conditions'].append({
                    'column': 'state',
                    'table': 'Customers',
                    'operator': '=',
                    'value': state.title()
                })
        
        # Price/amount conditions
        import re
        amount_match = re.search(r'over (\d+)|>(\d+)|under (\d+)|<(\d+)', question_lower)
        if amount_match:
            value = next(int(x) for x in amount_match.groups() if x is not None)
            operator = '>' if 'over' in question_lower or '>' in question_lower else '<'
            
            if 'order' in question_lower:
                extracted['conditions'].append({
                    'column': 'total_amount',
                    'table': 'Orders',
                    'operator': operator,
                    'value': value
                })
                if 'Orders' not in extracted['tables']:
                    extracted['tables'].append('Orders')
            elif 'product' in question_lower:
                extracted['conditions'].append({
                    'column': 'price',
                    'table': 'Products',
                    'operator': operator,
                    'value': value
                })
                if 'Products' not in extracted['tables']:
                    extracted['tables'].append('Products')
        
        return extracted
    
    def _build_query(self, extracted_info: Dict) -> str:
        """Build optimized SQL query"""
        if not extracted_info['tables']:
            return "SELECT * FROM Customers;"
        
        # Handle JOIN queries
        if extracted_info['requires_join'] and len(extracted_info['tables']) >= 2:
            return self._build_join_query(extracted_info)
        
        # Simple single table query
        main_table = extracted_info['tables'][0]
        
        # Build SELECT clause
        if extracted_info['aggregates']:
            select_clause = f"SELECT {extracted_info['aggregates'][0]}(*)"
        else:
            select_clause = "SELECT *"
        
        # FROM clause
        from_clause = f"FROM {main_table}"
        
        # Build WHERE clause
        where_parts = []
        params_placeholders = []
        
        for condition in extracted_info.get('conditions', []):
            if condition.get('table', main_table) == main_table:
                where_parts.append(f"{condition['column']} {condition['operator']} ?")
                params_placeholders.append('?')
        
        where_clause = ""
        if where_parts:
            where_clause = "WHERE " + " AND ".join(where_parts)
        
        # ORDER BY clause
        order_clause = ""
        if extracted_info['order_by']:
            if any(c['column'] == 'total_amount' for c in extracted_info.get('conditions', [])):
                order_clause = f"ORDER BY total_amount {extracted_info['order_by']}"
            else:
                order_clause = f"ORDER BY {main_table.lower()}_id {extracted_info['order_by']}"
        
        # LIMIT clause
        limit_clause = f"LIMIT {extracted_info['limit']}" if extracted_info['limit'] else ""
        
        # Combine clauses
        query_parts = [select_clause, from_clause]
        if where_clause:
            query_parts.append(where_clause)
        if order_clause:
            query_parts.append(order_clause)
        if limit_clause:
            query_parts.append(limit_clause)
        
        return " ".join(query_parts) + ";"
    
    def _build_join_query(self, extracted_info: Dict) -> str:
        """Build optimized JOIN query"""
        if 'Customers' in extracted_info['tables'] and 'Orders' in extracted_info['tables']:
            select_clause = "SELECT Customers.*, Orders.order_id, Orders.total_amount, Orders.status"
            from_clause = "FROM Customers"
            join_clause = "INNER JOIN Orders ON Customers.customer_id = Orders.customer_id"
        else:
            # Default to simple join
            select_clause = "SELECT *"
            from_clause = f"FROM {extracted_info['tables'][0]}"
            join_clause = f"INNER JOIN {extracted_info['tables'][1]} ON {extracted_info['tables'][0].lower()}_id = {extracted_info['tables'][1].lower()}_id"
        
        # Build WHERE clause
        where_parts = []
        for condition in extracted_info.get('conditions', []):
            where_parts.append(f"{condition['table']}.{condition['column']} {condition['operator']} ?")
        
        where_clause = ""
        if where_parts:
            where_clause = "WHERE " + " AND ".join(where_parts)
        
        # ORDER BY clause
        order_clause = ""
        if extracted_info['order_by']:
            if any(c['column'] == 'total_amount' for c in extracted_info.get('conditions', [])):
                order_clause = f"ORDER BY Orders.total_amount {extracted_info['order_by']}"
        
        # LIMIT clause
        limit_clause = f"LIMIT {extracted_info['limit']}" if extracted_info['limit'] else ""
        
        # Combine clauses
        query_parts = [select_clause, from_clause, join_clause]
        if where_clause:
            query_parts.append(where_clause)
        if order_clause:
            query_parts.append(order_clause)
        if limit_clause:
            query_parts.append(limit_clause)
        
        return " ".join(query_parts) + ";"
    
    def generate_sql(self, question: str) -> Tuple[str, list, bool]:
        """Generate and execute SQL - optimized version"""
        try:
            # Extract keywords
            extracted = self._extract_keywords(question)
            
            # Build query
            sql_query = self._build_query(extracted)
            
            # Execute with parameters
            results, success = self._execute_query_safe(sql_query, extracted.get('conditions', []))
            
            return sql_query, results, success
            
        except Exception as e:
            return f"-- Error: {str(e)}", [{"error": str(e)}], False
    
    def _execute_query_safe(self, sql_query: str, conditions: list) -> Tuple[list, bool]:
        """Execute query with proper error handling"""
        try:
            conn = sqlite3.connect(_DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Extract parameters
            params = [c['value'] for c in conditions if c['operator'] in ['>', '<', '=', '>=', '<=']]
            
            # Execute
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
            
            # Fetch results (limit to 100 rows for performance)
            rows = cursor.fetchmany(100)
            results = [dict(row) for row in rows]
            
            conn.close()
            return results, True
            
        except sqlite3.Error as e:
            return [{"error": f"Database error: {str(e)}"}], False

# ==================== HTML TEMPLATE (MINIFIED FOR SPEED) ====================
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NL to SQL</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box;}
        body{font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;padding:20px;}
        .container{max-width:1200px;margin:0 auto;background:#fff;border-radius:10px;box-shadow:0 20px 60px rgba(0,0,0,0.3);}
        .header{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:30px;text-align:center;}
        .header h1{font-size:2.5em;margin-bottom:10px;}
        .content{padding:30px;}
        .input-section{background:#f8f9fa;padding:20px;border-radius:8px;margin-bottom:30px;}
        .example-tag{display:inline-block;background:#fff;padding:8px 15px;margin:5px;border-radius:20px;color:#667eea;cursor:pointer;border:1px solid #dee2e6;}
        .example-tag:hover{background:#667eea;color:#fff;}
        .input-group{display:flex;gap:10px;margin:20px 0;}
        .input-group input{flex:1;padding:15px;border:2px solid #dee2e6;border-radius:6px;font-size:1em;}
        .input-group input:focus{outline:none;border-color:#667eea;}
        .input-group button{padding:15px 30px;background:#667eea;color:#fff;border:none;border-radius:6px;font-size:1em;font-weight:bold;cursor:pointer;}
        .input-group button:hover{background:#5a67d8;}
        .schema-info{background:#e3f2fd;padding:15px;border-radius:6px;font-size:0.9em;}
        .schema-info pre{background:#fff;padding:10px;border-radius:4px;margin-top:10px;}
        .sql-query{background:#1e1e1e;color:#d4d4d4;padding:20px;border-radius:8px;margin:20px 0;font-family:monospace;}
        .data-table{background:#fff;border-radius:8px;overflow:hidden;}
        .table-container{overflow-x:auto;max-height:400px;}
        table{width:100%;border-collapse:collapse;}
        th{background:#667eea;color:#fff;padding:12px;position:sticky;top:0;}
        td{padding:10px;border-bottom:1px solid #dee2e6;}
        tr:hover{background:#f8f9fa;}
        .loading{display:none;text-align:center;padding:20px;}
        .loading.active{display:block;}
        .spinner{border:4px solid #f3f3f3;border-top:4px solid #667eea;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;margin:0 auto;}
        @keyframes spin{0%{transform:rotate(0deg);}100%{transform:rotate(360deg);}}
        .error{background:#fee;color:#c33;padding:15px;border-radius:6px;}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Natural Language to SQL</h1>
            <p>Ask questions in English, get SQL and results!</p>
        </div>
        <div class="content">
            <div class="input-section">
                <div style="margin-bottom:15px;">
                    <span class="example-tag" onclick="setEx
