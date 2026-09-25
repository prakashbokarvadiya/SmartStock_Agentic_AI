# mcp_server.py

import os
from decimal import Decimal
from datetime import date, datetime

from sqlalchemy import create_engine
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP


# ============================================================
# LOAD ENV
# ============================================================

load_dotenv()


# ============================================================
# ENV VARIABLES
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))


# ============================================================
# MCP SERVER
# ============================================================

mcp = FastMCP(
    "Product Order MCP Server"
)


# ============================================================
# DATABASE CONNECTION (Pooled — avoids TCP handshake per query)
# ============================================================

if DATABASE_URL:
    _db_url = DATABASE_URL
    if _db_url.startswith("postgres://"):
        _db_url = _db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif _db_url.startswith("postgresql://"):
        _db_url = _db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    _db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

_db_engine = create_engine(_db_url, pool_size=10, max_overflow=5, pool_recycle=300)


class DictCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        if params is not None:
            return self._cursor.execute(query, params)
        return self._cursor.execute(query)

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return row
        if hasattr(self._cursor, "description") and self._cursor.description:
            colnames = [col[0] for col in self._cursor.description]
            return dict(zip(colnames, row))
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return rows
        if hasattr(self._cursor, "description") and self._cursor.description:
            colnames = [col[0] for col in self._cursor.description]
            return [dict(zip(colnames, r)) for r in rows]
        return rows

    def close(self):
        return self._cursor.close()

    def __iter__(self):
        return iter(self.fetchall())


class PooledConnection:
    def __init__(self, raw_conn):
        self._conn = raw_conn
        self.closed = False

    def cursor(self, dictionary=True, cursor_factory=None):
        return DictCursorWrapper(self._conn.cursor())

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            self.closed = True

    def is_connected(self):
        return self._conn is not None and not self.closed


def get_db_connection():
    """Return a connection from the pool (no new TCP handshake)."""
    return PooledConnection(_db_engine.raw_connection())


# ============================================================
# SERIALIZE MYSQL DATA
# ============================================================

def serialize_data(data):

    if isinstance(data, list):

        return [
            serialize_data(item)
            for item in data
        ]

    if isinstance(data, dict):

        return {
            key: serialize_data(value)
            for key, value in data.items()
        }

    if isinstance(data, (datetime, date)):

        return data.isoformat()

    if isinstance(data, Decimal):

        return float(data)

    return data


# ============================================================
# PRODUCT TOOLS
# ============================================================


@mcp.tool()
def get_products():

    """
    Get all products from the products table.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute("""
            SELECT
                product_id,
                product_name,
                category,
                brand,
                price,
                stock
            FROM products
            ORDER BY product_id
        """)

        products = cursor.fetchall()

        return serialize_data(products)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# GET SINGLE PRODUCT
# ============================================================

@mcp.tool()
def get_product(product_id: str):

    """
    Get a single product using product_id.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                category,
                brand,
                price,
                stock
            FROM products
            WHERE product_id = %s
            """,
            (product_id,)
        )

        product = cursor.fetchone()

        if not product:

            return {
                "message": "Product not found",
                "product_id": product_id
            }

        return serialize_data(product)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# SEARCH PRODUCTS
# ============================================================

@mcp.tool()
def search_products(query: str):

    """
    Search products by product ID, product name,
    category or brand.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        search_value = f"%{query}%"

        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                category,
                brand,
                price,
                stock
            FROM products
            WHERE
                product_id LIKE %s
                OR product_name LIKE %s
                OR category LIKE %s
                OR brand LIKE %s
            ORDER BY product_id
            """,
            (
                search_value,
                search_value,
                search_value,
                search_value
            )
        )

        products = cursor.fetchall()

        return serialize_data(products)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# LOW STOCK PRODUCTS
# ============================================================

@mcp.tool()
def get_low_stock_products(limit: int = 10):

    """
    Get products whose stock is less than or equal to
    the supplied limit.
    """

    db = None
    cursor = None

    try:

        if limit < 0:
            return {
                "error": "Limit cannot be negative"
            }

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                category,
                brand,
                price,
                stock
            FROM products
            WHERE stock <= %s
            ORDER BY stock ASC
            """,
            (limit,)
        )

        products = cursor.fetchall()

        return serialize_data(products)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDER TOOLS
# ============================================================


@mcp.tool()
def get_orders():

    """
    Get all orders from the orders table.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute("""
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                order_date,
                order_status
            FROM orders
            ORDER BY order_id
        """)

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# GET SINGLE ORDER
# ============================================================

@mcp.tool()
def get_order(order_id: str):

    """
    Get a single order using order_id.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                order_date,
                order_status
            FROM orders
            WHERE order_id = %s
            """,
            (order_id,)
        )

        order = cursor.fetchone()

        if not order:

            return {
                "message": "Order not found",
                "order_id": order_id
            }

        return serialize_data(order)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# SEARCH ORDERS (JOINS orders + products)
# ============================================================

@mcp.tool()
def search_orders(query: str):

    """
    Search orders by order ID, customer ID, product ID,
    product name, category, brand, or order status.
    Joins the orders and products tables so you can search
    for orders of a specific category (e.g. Smartphone, Television)
    or brand (e.g. Sony, Samsung).
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        search_value = f"%{query}%"

        cursor.execute(
            """
            SELECT
                o.order_id,
                o.customer_id,
                o.product_id,
                p.product_name,
                p.category,
                p.brand,
                p.price,
                o.quantity,
                o.order_date,
                o.order_status
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE
                o.order_id LIKE %s
                OR o.customer_id LIKE %s
                OR o.product_id LIKE %s
                OR o.order_status LIKE %s
                OR p.product_name LIKE %s
                OR p.category LIKE %s
                OR p.brand LIKE %s
            ORDER BY o.order_id
            """,
            (
                search_value,
                search_value,
                search_value,
                search_value,
                search_value,
                search_value,
                search_value
            )
        )

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDERS BY CATEGORY OR PRODUCT NAME
# ============================================================

@mcp.tool()
def get_orders_by_category(category_or_name: str):

    """
    Get all orders for products belonging to a specific category
    (e.g., Smartphone, Television, Laptop, Audio, Electronics)
    or product name/brand.
    Joins orders and products tables.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        search_value = f"%{category_or_name}%"

        cursor.execute(
            """
            SELECT
                o.order_id,
                o.customer_id,
                o.product_id,
                p.product_name,
                p.category,
                p.brand,
                p.price,
                o.quantity,
                o.order_date,
                o.order_status
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE
                p.category LIKE %s
                OR p.product_name LIKE %s
                OR p.brand LIKE %s
            ORDER BY o.order_id
            """,
            (
                search_value,
                search_value,
                search_value
            )
        )

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDERS BY STATUS
# ============================================================

@mcp.tool()
def get_orders_by_status(status: str):

    """
    Get orders by exact order status.

    Allowed statuses:
    Confirmed
    Processing
    Shipped
    Delivered
    Returned
    Cancelled
    """

    allowed_statuses = {
        "Confirmed",
        "Processing",
        "Shipped",
        "Delivered",
        "Returned",
        "Cancelled"
    }

    if status not in allowed_statuses:

        return {
            "error": "Invalid order status",
            "allowed_statuses": list(
                allowed_statuses
            )
        }

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                order_date,
                order_status
            FROM orders
            WHERE order_status = %s
            ORDER BY order_id
            """,
            (status,)
        )

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDERS STATUS SUMMARY (TOKEN OPTIMIZED)
# ============================================================

@mcp.tool()
def get_orders_status_summary(status: str):

    """
    Get total order count and sample records for a specific order status
    (Confirmed, Processing, Shipped, Delivered, Returned, Cancelled).
    Use this tool whenever the user asks for order counts, totals, or list of orders
    for a specific status (e.g. 'Cancelled order kitne hain', 'Delivered orders count').
    This tool is token-optimized and returns total count + top sample records with product names.
    """

    allowed_statuses = {
        "Confirmed",
        "Processing",
        "Shipped",
        "Delivered",
        "Returned",
        "Cancelled"
    }

    if status not in allowed_statuses:
        return {
            "error": "Invalid order status",
            "allowed_statuses": list(allowed_statuses)
        }

    db = None
    cursor = None

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Count total matching orders
        cursor.execute(
            "SELECT COUNT(*) AS total_count FROM orders WHERE order_status = %s",
            (status,)
        )
        count_res = cursor.fetchone()
        total_count = count_res["total_count"] if count_res else 0

        # Fetch top 5 sample orders joined with products for product names
        cursor.execute(
            """
            SELECT
                o.order_id,
                o.customer_id,
                o.product_id,
                p.product_name,
                p.category,
                p.brand,
                p.price,
                o.quantity,
                o.order_date
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_status = %s
            ORDER BY o.order_id
            LIMIT 5
            """,
            (status,)
        )

        samples = cursor.fetchall()

        return serialize_data({
            "order_status": status,
            "total_orders_count": total_count,
            "showing_sample_records_count": len(samples),
            "sample_orders": samples,
            "note": f"Total {total_count} {status} orders exist in database. Top sample records are included above."
        })

    except Exception as e:
        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:
        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDERS BY CUSTOMER
# ============================================================

@mcp.tool()
def get_orders_by_customer(
    customer_id: str
):

    """
    Get all orders belonging to a customer.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                order_date,
                order_status
            FROM orders
            WHERE customer_id = %s
            ORDER BY order_date DESC
            """,
            (customer_id,)
        )

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# ORDERS BY PRODUCT
# ============================================================

@mcp.tool()
def get_product_orders(
    product_id: str
):

    """
    Get all orders associated with a product.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                order_date,
                order_status
            FROM orders
            WHERE product_id = %s
            ORDER BY order_date DESC
            """,
            (product_id,)
        )

        orders = cursor.fetchall()

        return serialize_data(orders)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# DATABASE SUMMARY
# ============================================================

@mcp.tool()
def get_database_summary():

    """
    Return a summary of products and orders in a single optimized query.
    """

    db = None
    cursor = None

    try:

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        # Single query: products count + stock sum together (avoid 2 round-trips)
        cursor.execute(
            "SELECT COUNT(*) AS total_products, COALESCE(SUM(stock), 0) AS total_stock FROM products"
        )
        products_row = cursor.fetchone()

        # Single query: derive total_orders + per-status breakdown together
        cursor.execute("""
            SELECT
                order_status,
                COUNT(*) AS total
            FROM orders
            GROUP BY order_status
            ORDER BY order_status
        """)
        status_summary = cursor.fetchall()

        total_orders = sum(r["total"] for r in status_summary) if status_summary else 0

        return serialize_data({
            "total_products": products_row["total_products"],
            "total_orders": total_orders,
            "total_stock": products_row["total_stock"],
            "orders_by_status": status_summary
        })

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# DYNAMIC SQL QUERY EXECUTION TOOL
# ============================================================

@mcp.tool()
def execute_sql_query(query: str):

    """
    Execute a dynamic read-only SQL query against the PostgreSQL database.
    Use this tool to generate and run custom SQL queries for ANY analytical or complex
    user question (e.g. joins, total sales, revenue, averages, counts, min/max, category grouping).

    DATABASE SCHEMA:
    Table `products`:
      - product_id (VARCHAR(10), PRIMARY KEY, e.g. 'P001')
      - product_name (VARCHAR(150), e.g. 'Samsung Smart TV')
      - category (VARCHAR(80), e.g. 'Television', 'Smartphone', 'Laptop')
      - brand (VARCHAR(80), e.g. 'Samsung', 'LG', 'Sony')
      - price (DECIMAL(12,2))
      - stock (INT)

    Table `orders`:
      - order_id (VARCHAR(10), PRIMARY KEY, e.g. 'O1001')
      - customer_id (VARCHAR(10), e.g. 'C101')
      - product_id (VARCHAR(10), FOREIGN KEY references products.product_id)
      - quantity (INT)
      - order_date (DATE, format 'YYYY-MM-DD')
      - order_status (VARCHAR: 'Confirmed', 'Processing', 'Shipped', 'Delivered', 'Returned', 'Cancelled')

    RULES:
    - Only SELECT queries are allowed.
    - JOIN orders and products on orders.product_id = products.product_id when needed.
    - Always add LIMIT (e.g. LIMIT 20) unless doing aggregate/COUNT queries.
    - This is PostgreSQL syntax — use standard SQL, not MySQL-specific functions.
    """

    db = None
    cursor = None

    try:

        clean_query = query.strip()
        first_word = clean_query.split()[0].upper() if clean_query else ""

        if first_word not in ("SELECT", "WITH", "EXPLAIN"):
            return {
                "error": "Only read-only SELECT queries are allowed."
            }

        # Security check for mutation keywords
        forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
        upper_q = clean_query.upper()
        for word in forbidden:
            if f" {word} " in f" {upper_q} ":
                return {
                    "error": f"Security restriction: Operation '{word}' is not allowed."
                }

        db = get_db_connection()

        cursor = db.cursor(
            dictionary=True
        )

        cursor.execute(clean_query)

        rows = cursor.fetchall()
        # Cap result set to prevent unbounded token usage
        if isinstance(rows, list) and len(rows) > 100:
            rows = rows[:100]

        return serialize_data(rows)

    except Exception as e:

        return {
            "error": f"PostgreSQL Error: {str(e)}"
        }

    finally:

        if cursor:
            cursor.close()

        if db and db.is_connected():
            db.close()


# ============================================================
# MCP SERVER START
# ============================================================

if __name__ == "__main__":

    mcp.run()