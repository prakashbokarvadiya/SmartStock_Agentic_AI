# =============================================================================
# email_automate.py
# Live Low-Stock Email Automation  --  ADD-ONLY module
# All new functionality. Zero changes to existing app.py code.
# =============================================================================
import os
import json
import pickle
import base64
import logging
import asyncio
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from sqlalchemy import create_engine
from dotenv import load_dotenv
from google import genai
from google.genai import types as genai_types
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("email_automate")

# ---------------------------------------------------------------------------
# Env  (all values from .env — nothing hard-coded)
# ---------------------------------------------------------------------------
load_dotenv()
LOW_STOCK_THRESHOLD  = int(os.getenv("LOW_STOCK_THRESHOLD", "3"))
STOCK_CHECK_INTERVAL = int(os.getenv("STOCK_CHECK_INTERVAL", "300"))
ADMIN_EMAIL          = os.getenv("ADMIN_EMAIL", "thakorhim@gmail.com")
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL         = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")
DATABASE_URL         = os.getenv("DATABASE_URL")
POSTGRES_HOST        = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_USER        = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD    = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DATABASE    = os.getenv("POSTGRES_DATABASE", "")
POSTGRES_PORT        = int(os.getenv("POSTGRES_PORT", "5432"))

BASE_DIR         = Path(__file__).resolve().parent
CREDENTIALS_FILE = BASE_DIR / "credentials.json"
TOKEN_FILE       = BASE_DIR / "token.pickle"
GMAIL_SCOPES     = ["https://www.googleapis.com/auth/gmail.send"]


# ===========================================================================
# DATABASE HELPERS  (SQLAlchemy connection pool)
# ===========================================================================

if DATABASE_URL:
    _db_url = DATABASE_URL
    if _db_url.startswith("postgres://"):
        _db_url = _db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif _db_url.startswith("postgresql://"):
        _db_url = _db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    _db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

_db_engine = create_engine(_db_url, pool_size=5, max_overflow=5, pool_recycle=300)


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
    return PooledConnection(_db_engine.raw_connection())


_alerts_table_ready = False


def ensure_alerts_table():
    """Create inventory_alerts table if not exists. Runs only once (idempotent guard)."""
    global _alerts_table_ready
    if _alerts_table_ready:
        return
    ddl = (
        "CREATE TABLE IF NOT EXISTS inventory_alerts ("
        "id SERIAL PRIMARY KEY,"
        "product_id VARCHAR(10) NOT NULL,"
        "alert_type VARCHAR(50) NOT NULL,"
        "stock_level INT NOT NULL,"
        "alert_sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "CONSTRAINT unique_product_alert UNIQUE (product_id, alert_type),"
        "CONSTRAINT fk_inventory_alert_product "
        "FOREIGN KEY (product_id) REFERENCES products(product_id) "
        "ON DELETE CASCADE)"
    )
    db = get_db_connection()
    try:
        cur = db.cursor()
        cur.execute(ddl)
        db.commit()
        cur.close()
        _alerts_table_ready = True
        logger.info("inventory_alerts table ready.")
    except Exception as exc:
        logger.error(f"ensure_alerts_table failed: {exc}")
    finally:
        if db and not db.closed:
            db.close()


# ===========================================================================
# STOCK QUERIES
# ===========================================================================

def get_low_stock_products():
    db = get_db_connection()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM products WHERE stock <= %s ORDER BY stock ASC",
            (LOW_STOCK_THRESHOLD,))
        rows = cur.fetchall()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    for row in rows:
        if row.get("price") is not None:
            row["price"] = float(row["price"])
    return rows


def get_product_by_id(product_id):
    db = get_db_connection()
    try:
        cur = db.cursor()
        cur.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
        row = cur.fetchone()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    if row and row.get("price") is not None:
        row["price"] = float(row["price"])
    return row


# ===========================================================================
# ALERT STATE  (inventory_alerts table)
# ===========================================================================

def is_alert_sent(product_id, alert_type="low_stock"):
    db = get_db_connection()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM inventory_alerts WHERE product_id=%s AND alert_type=%s",
            (product_id, alert_type))
        result = cur.fetchone()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    return result is not None


def mark_alert_sent(product_id, stock_level, alert_type="low_stock"):
    db = get_db_connection()
    try:
        cur = db.cursor()
        sql = (
            "INSERT INTO inventory_alerts "
            "(product_id, alert_type, stock_level, alert_sent_at) "
            "VALUES (%s, %s, %s, NOW()) "
            "ON CONFLICT (product_id, alert_type) DO UPDATE SET "
            "stock_level = EXCLUDED.stock_level, alert_sent_at = NOW()"
        )
        cur.execute(sql, (product_id, alert_type, stock_level))
        db.commit()
        cur.close()
        logger.info(f"Alert marked sent for {product_id} (stock={stock_level}).")
    finally:
        if db and not db.closed:
            db.close()


def reset_alert(product_id, alert_type="low_stock"):
    db = get_db_connection()
    try:
        cur = db.cursor()
        cur.execute(
            "DELETE FROM inventory_alerts WHERE product_id=%s AND alert_type=%s",
            (product_id, alert_type))
        deleted = cur.rowcount
        db.commit()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    if deleted > 0:
        logger.info(f"Alert RESET for {product_id} — stock restored above threshold.")


def reset_restocked_alerts():
    """Bug fix: removed cursor_factory=RealDictCursor — DictCursorWrapper handles dict rows."""
    db = get_db_connection()
    try:
        cur = db.cursor()   # DictCursorWrapper returns dict rows natively
        cur.execute(
            "SELECT p.product_id FROM products p "
            "JOIN inventory_alerts ia "
            "ON p.product_id=ia.product_id AND ia.alert_type='low_stock' "
            "WHERE p.stock > %s",
            (LOW_STOCK_THRESHOLD,))
        rows = cur.fetchall()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    for row in rows:
        reset_alert(row["product_id"])


# ===========================================================================
# GEMINI  --  email content generation (reuses existing GEMINI_API_KEY)
# ===========================================================================

def generate_low_stock_email(products):
    client = genai.Client(api_key=GEMINI_API_KEY)
    lines = []
    for p in products:
        lines.append(
            "  Product ID : " + str(p["product_id"]) + "\n"
            "  Name       : " + str(p["product_name"]) + "\n"
            "  Category   : " + str(p["category"]) + "\n"
            "  Brand      : " + str(p["brand"]) + "\n"
            "  Stock      : " + str(p["stock"]) + " units  CRITICAL LOW\n"
            "  Price      : Rs. " + f"{p['price']:.2f}"
        )
    product_block = "\n\n".join(lines)
    count = len(products)
    prompt = (
        "You are an inventory management system. Write a professional "
        "low-stock alert email for an electronics store administrator.\n\n"
        "The following " + str(count) + " product(s) have critically low stock "
        "(threshold: " + str(LOW_STOCK_THRESHOLD) + " units or fewer):\n\n"
        + product_block + "\n\n"
        "Rules:\n"
        "- Use ONLY the data above. Do NOT invent anything.\n"
        "- Professional, urgent but concise.\n"
        "- Recommend immediate restocking.\n"
        "- Plain text only, no HTML, no markdown.\n\n"
        'Return ONLY valid JSON: {"subject": "...", "body": "..."}\n'
        "No explanation, no code fences."
    )
    logger.info(f"Generating email for {count} product(s) via Gemini.")
    resp = client.models.generate_content(
        model=GEMINI_MODEL, contents=prompt,
        config=genai_types.GenerateContentConfig(temperature=0.2))
    raw = resp.text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


# ===========================================================================
# GMAIL API
# ===========================================================================

def get_gmail_service():
    creds = None
    if TOKEN_FILE.exists():
        try:
            with open(TOKEN_FILE, "rb") as f:
                creds = pickle.load(f)
        except Exception as e:
            logger.warning(f"Error loading token.pickle: {e}. Removing corrupt token file.")
            if TOKEN_FILE.exists():
                try:
                    TOKEN_FILE.unlink()
                except Exception:
                    pass

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing Gmail OAuth token.")
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Failed to refresh token ({e}). Removing expired/revoked token for fresh OAuth authentication.")
                creds = None
                if TOKEN_FILE.exists():
                    try:
                        TOKEN_FILE.unlink()
                    except Exception:
                        pass

        if not creds or not creds.valid:
            if not CREDENTIALS_FILE.exists():
                raise FileNotFoundError(
                    "credentials.json not found at " + str(CREDENTIALS_FILE) + ".\n"
                    "Download from Google Cloud Console > APIs & Services > Credentials.\n"
                    "Create OAuth 2.0 Client ID (Desktop app) with Gmail API enabled.")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
            logger.info("Gmail OAuth consent done.")

        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
        logger.info("Token saved: " + str(TOKEN_FILE))

    return build("gmail", "v1", credentials=creds)


def send_email(subject, body, to_email=None):
    if to_email is None:
        to_email = ADMIN_EMAIL
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"]      = to_email
    msg["From"]    = "me"
    msg.attach(MIMEText(body, "plain"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    logger.info("Email sent -> " + to_email + " | " + subject)


# ===========================================================================
# HIGH-LEVEL CHECK FUNCTIONS
# ===========================================================================

def check_low_stock_for_product(product_id):
    # Single-product stock check.
    # Called after product update or order creation from app.py.
    # Errors are LOGGED -- never raised. HTTP response of caller unaffected.
    # Flow:
    #   stock > threshold  =>  reset_alert      (restock event)
    #   stock <= threshold =>  is_alert_sent?
    #                            YES -> skip     (duplicate prevention)
    #                            NO  -> email -> mark_alert_sent
    try:
        ensure_alerts_table()
        product = get_product_by_id(product_id)
        if not product:
            logger.warning("check_low_stock_for_product: " + product_id + " not found.")
            return
        stock = product["stock"]
        logger.info(
            "Low-stock check | " + product_id +
            " stock=" + str(stock) +
            " threshold=" + str(LOW_STOCK_THRESHOLD))
        if stock > LOW_STOCK_THRESHOLD:
            reset_alert(product_id)
            return
        if is_alert_sent(product_id):
            logger.info("Alert already sent for " + product_id + " -- skipping.")
            return
        logger.info(
            "LOW STOCK DETECTED: " + product_id +
            " (" + product["product_name"] + ")" +
            " stock=" + str(stock))
        email_content = generate_low_stock_email([product])
        logger.info("Sending low-stock email for " + product_id + ".")
        send_email(email_content["subject"], email_content["body"])
        mark_alert_sent(product_id, stock)
    except Exception as exc:
        logger.error("check_low_stock_for_product(" + product_id + ") error: " + str(exc))


def check_low_stock_products():
    # All-products check. Called by background monitor.
    # ONE consolidated email for all newly low-stock products (Requirement 9).
    try:
        logger.info("Running full low-stock check cycle...")
        low_stock = get_low_stock_products()
        if not low_stock:
            logger.info("No low-stock products.")
            return
        new_alerts = [p for p in low_stock if not is_alert_sent(p["product_id"])]
        if not new_alerts:
            logger.info(
                str(len(low_stock)) + " low-stock product(s) -- all already alerted.")
            return
        logger.info("New alerts: " + str([p["product_id"] for p in new_alerts]))
        email_content = generate_low_stock_email(new_alerts)
        send_email(email_content["subject"], email_content["body"])
        for p in new_alerts:
            mark_alert_sent(p["product_id"], p["stock"])
        logger.info("Consolidated alert sent for " + str(len(new_alerts)) + " product(s).")
    except Exception as exc:
        logger.error("check_low_stock_products error: " + str(exc))


# ===========================================================================
# ASYNC BACKGROUND MONITOR
# ===========================================================================

async def run_stock_monitor():
    # Async background task started in FastAPI lifespan.
    # Every STOCK_CHECK_INTERVAL seconds (default 300 = 5 min):
    #   1. Reset alerts for products back above threshold (restock reset)
    #   2. Send consolidated email for newly low-stock products
    # Errors in each cycle are logged, not raised -- monitor never crashes.
    logger.info(
        "Stock monitor started | interval=" + str(STOCK_CHECK_INTERVAL) + "s | "
        "threshold=" + str(LOW_STOCK_THRESHOLD) + " | admin=" + ADMIN_EMAIL)
    try:
        ensure_alerts_table()
    except Exception as exc:
        logger.error("inventory_alerts table creation failed: " + str(exc))

    while True:
        try:
            await asyncio.sleep(STOCK_CHECK_INTERVAL)
            await asyncio.to_thread(reset_restocked_alerts)
            await asyncio.to_thread(check_low_stock_products)
        except asyncio.CancelledError:
            logger.info("Stock monitor cancelled -- clean shutdown.")
            raise
        except Exception as exc:
            logger.error("Stock monitor cycle error (will retry): " + str(exc))


# ===========================================================================
# ONE-TIME SETUP  --  python email_automate.py
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("email_automate.py  --  One-time Gmail Setup")
    print("=" * 60)
    print("\n[1] Creating inventory_alerts table...")
    ensure_alerts_table()
    print("    Done.")
    print("\n[2] Gmail OAuth consent (browser will open)...")
    try:
        get_gmail_service()
        print("    Authorized. token.pickle saved: " + str(TOKEN_FILE))
    except FileNotFoundError as e:
        print("\n    ERROR: " + str(e))
        raise SystemExit(1)
    print("\n[3] Sending verification email...")
    send_email(
        subject="[Test] ElectraStore Low-Stock System -- Setup OK",
        body=(
            "Your Gmail API integration is working correctly.\n\n"
            "Admin email    : " + ADMIN_EMAIL + "\n"
            "Threshold      : " + str(LOW_STOCK_THRESHOLD) + " units\n"
            "Check interval : " + str(STOCK_CHECK_INTERVAL) + " seconds\n"
        ),
    )
    print("    Verification email sent to " + ADMIN_EMAIL + ".")
    print("\nSetup complete. Start: python -m uvicorn app:app --reload")
