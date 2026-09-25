# =============================================================================
# email_automate.py
# Live Low-Stock Email Automation + Inventory Alerts Tracker
# Supports SMTP (Gmail App Password) for Cloud/Docker/Render & Gmail OAuth API
# =============================================================================
import os
import json
import pickle
import base64
import logging
import asyncio
import smtplib
import ssl
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("email_automate")

# ---------------------------------------------------------------------------
# Env (all values from .env — nothing hard-coded)
# ---------------------------------------------------------------------------
load_dotenv()
LOW_STOCK_THRESHOLD  = int(os.getenv("LOW_STOCK_THRESHOLD", "3"))
STOCK_CHECK_INTERVAL = int(os.getenv("STOCK_CHECK_INTERVAL", "300"))
ADMIN_EMAIL          = os.getenv("ADMIN_EMAIL", "thakorhim@gmail.com")
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL         = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
DATABASE_URL         = os.getenv("DATABASE_URL")
POSTGRES_HOST        = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_USER        = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD    = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DATABASE    = os.getenv("POSTGRES_DATABASE", "")
POSTGRES_PORT        = int(os.getenv("POSTGRES_PORT", "5432"))

# SMTP configuration (Recommended for Render / Docker / Cloud)
SMTP_SERVER          = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT            = int(os.getenv("SMTP_PORT", "587"))
ADMIN_EMAIL          = os.getenv("ADMIN_EMAIL", "thakorhim@gmail.com").strip()
SMTP_USER            = (os.getenv("SMTP_USER") or os.getenv("EMAIL_USER") or ADMIN_EMAIL).strip()
SMTP_PASSWORD        = (
    os.getenv("SMTP_PASSWORD")
    or os.getenv("GMAIL_APP_PASSWORD")
    or os.getenv("EMAIL_PASSWORD")
    or os.getenv("EMAIL_APP_PASSWORD")
    or ""
).replace(" ", "").strip()

# Gmail OAuth configuration (Alternative for OAuth flows)
BASE_DIR             = Path(__file__).resolve().parent
CREDENTIALS_FILE     = BASE_DIR / "credentials.json"
TOKEN_FILE           = BASE_DIR / "token.pickle"
GMAIL_SCOPES         = ["https://www.googleapis.com/auth/gmail.send"]
GMAIL_TOKEN_BASE64   = os.getenv("GMAIL_TOKEN_BASE64", "")
GMAIL_REFRESH_TOKEN  = os.getenv("GMAIL_REFRESH_TOKEN", "")
GMAIL_CLIENT_ID      = os.getenv("GMAIL_CLIENT_ID", "")
GMAIL_CLIENT_SECRET  = os.getenv("GMAIL_CLIENT_SECRET", "")


# ===========================================================================
# DATABASE HELPERS (SQLAlchemy connection pool)
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

    @property
    def rowcount(self):
        return self._cursor.rowcount

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
    """Create inventory_alerts table if not exists. Runs once (idempotent guard)."""
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
    db = None
    try:
        db = get_db_connection()
        cur = db.cursor()
        cur.execute(ddl)
        db.commit()
        cur.close()
        _alerts_table_ready = True
        logger.info("inventory_alerts table ready in database.")
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
    rows = []
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM products WHERE stock <= %s ORDER BY stock ASC",
            (LOW_STOCK_THRESHOLD,)
        )
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
    row = None
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
# ALERT STATE (inventory_alerts table)
# ===========================================================================

def is_alert_sent(product_id, alert_type="low_stock"):
    db = get_db_connection()
    result = None
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM inventory_alerts WHERE product_id=%s AND alert_type=%s",
            (product_id, alert_type)
        )
        result = cur.fetchone()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    return result is not None


def mark_alert_sent(product_id, stock_level, alert_type="low_stock"):
    """Persists alert record into inventory_alerts table immediately."""
    db = None
    try:
        ensure_alerts_table()
        db = get_db_connection()
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
        logger.info(f"Inventory alert SAVED in DB for {product_id} (stock={stock_level}).")
    except Exception as exc:
        logger.error(f"Failed to mark alert in DB for {product_id}: {exc}")
        if db:
            db.rollback()
    finally:
        if db and not db.closed:
            db.close()


def reset_alert(product_id, alert_type="low_stock"):
    db = get_db_connection()
    deleted = 0
    try:
        cur = db.cursor()
        cur.execute(
            "DELETE FROM inventory_alerts WHERE product_id=%s AND alert_type=%s",
            (product_id, alert_type)
        )
        deleted = cur.rowcount
        db.commit()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    if deleted > 0:
        logger.info(f"Alert RESET for {product_id} — stock restored above threshold.")


def reset_restocked_alerts():
    """Clear alerts for products whose stock was increased above threshold."""
    db = get_db_connection()
    rows = []
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT p.product_id FROM products p "
            "JOIN inventory_alerts ia "
            "ON p.product_id=ia.product_id AND ia.alert_type='low_stock' "
            "WHERE p.stock > %s",
            (LOW_STOCK_THRESHOLD,)
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        if db and not db.closed:
            db.close()
    for row in rows:
        reset_alert(row["product_id"])


# ===========================================================================
# GEMINI -- email content generation (with fallback)
# ===========================================================================

def generate_low_stock_email(products):
    """Generates professional email body using Gemini with a reliable fallback."""
    count = len(products)
    try:
        from google import genai
        from google.genai import types as genai_types

        client = genai.Client(api_key=GEMINI_API_KEY)
        lines = []
        for p in products:
            lines.append(
                f"  Product ID : {p.get('product_id')}\n"
                f"  Name       : {p.get('product_name')}\n"
                f"  Category   : {p.get('category')}\n"
                f"  Brand      : {p.get('brand')}\n"
                f"  Stock      : {p.get('stock')} units  CRITICAL LOW\n"
                f"  Price      : Rs. {p.get('price', 0):.2f}"
            )
        product_block = "\n\n".join(lines)
        prompt = (
            "You are an inventory management system. Write a professional "
            "low-stock alert email for an electronics store administrator.\n\n"
            f"The following {count} product(s) have critically low stock "
            f"(threshold: {LOW_STOCK_THRESHOLD} units or fewer):\n\n"
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
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(temperature=0.2)
        )
        raw = resp.text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except Exception as exc:
        logger.warning(f"Gemini email generation fallback ({exc}). Using standard template.")
        # Reliable fallback content
        product_list_str = "\n".join([
            f"• [{p.get('product_id')}] {p.get('product_name')} | Stock: {p.get('stock')} left | Price: Rs. {p.get('price', 0)}"
            for p in products
        ])
        subject = f"⚠️ [URGENT] Low Stock Alert: {count} Product(s) Below Threshold"
        body = (
            f"Hello Administrator,\n\n"
            f"This is an automated low-stock alert from SmartStock AI.\n"
            f"The following {count} product(s) have reached or fallen below the threshold ({LOW_STOCK_THRESHOLD} units):\n\n"
            f"{product_list_str}\n\n"
            f"Please review and initiate restocking immediately.\n\n"
            f"Best regards,\nSmartStock Agentic AI System"
        )
        return {"subject": subject, "body": body}


# ===========================================================================
# EMAIL DISPATCH: SMTP (Cloud/Docker/Render) & Gmail OAuth API
# ===========================================================================

def send_email_smtp(subject: str, body: str, to_email: str) -> bool:
    """Send email using standard SMTP (Works seamlessly on Render / Docker / Cloud)."""
    if not SMTP_PASSWORD:
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"]      = to_email
    msg["From"]    = SMTP_USER
    msg.attach(MIMEText(body, "plain"))

    logger.info(f"Connecting to SMTP server {SMTP_SERVER}:{SMTP_PORT} as {SMTP_USER}...")
    if SMTP_PORT == 465:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_email, msg.as_string())
    else:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_email, msg.as_string())

    logger.info(f"Email sent via SMTP successfully -> {to_email} | {subject}")
    return True


def get_gmail_oauth_service():
    """Retrieve Gmail OAuth service from token, env var, or credentials."""
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    creds = None

    # Option 1: Base64 token in environment variable
    if GMAIL_TOKEN_BASE64:
        try:
            token_bytes = base64.b64decode(GMAIL_TOKEN_BASE64)
            creds = pickle.loads(token_bytes)
        except Exception as e:
            logger.warning(f"Failed to load GMAIL_TOKEN_BASE64: {e}")

    # Option 2: token.pickle file on disk
    if not creds and TOKEN_FILE.exists():
        try:
            with open(TOKEN_FILE, "rb") as f:
                creds = pickle.load(f)
        except Exception as e:
            logger.warning(f"Error loading token.pickle: {e}")

    # Option 3: Refresh token in env vars
    if not creds and GMAIL_REFRESH_TOKEN and GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET:
        try:
            creds = Credentials(
                None,
                refresh_token=GMAIL_REFRESH_TOKEN,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=GMAIL_CLIENT_ID,
                client_secret=GMAIL_CLIENT_SECRET,
                scopes=GMAIL_SCOPES
            )
        except Exception as e:
            logger.warning(f"Failed to build OAuth credentials from refresh token: {e}")

    # Refresh expired credentials if refresh token is available
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            logger.warning(f"Failed to refresh Gmail OAuth token: {e}")
            creds = None

    # Option 4: Local interactive consent flow (Local Dev only with display)
    if not creds or not creds.valid:
        if CREDENTIALS_FILE.exists() and os.isatty(0):
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, "wb") as f:
                pickle.dump(creds, f)
            logger.info(f"Gmail OAuth consent complete. Saved {TOKEN_FILE}")

    if not creds or not creds.valid:
        raise ValueError("Gmail OAuth credentials not configured or expired.")

    return build("gmail", "v1", credentials=creds)


def send_email(subject: str, body: str, to_email: str = None):
    """
    Sends email. Tries SMTP (App Password) first, then Gmail OAuth API.
    Raises exception if both fail so calling function can log it.
    """
    if to_email is None:
        to_email = ADMIN_EMAIL

    # Method 1: Try SMTP if SMTP_PASSWORD / GMAIL_APP_PASSWORD is set
    if SMTP_PASSWORD:
        try:
            if send_email_smtp(subject, body, to_email):
                return
        except Exception as smtp_err:
            logger.warning(f"SMTP send failed: {smtp_err}. Trying Gmail OAuth fallback...")

    # Method 2: Try Gmail OAuth API
    try:
        service = get_gmail_oauth_service()
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["To"]      = to_email
        msg["From"]    = "me"
        msg.attach(MIMEText(body, "plain"))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.info(f"Email sent via Gmail API -> {to_email} | {subject}")
        return
    except Exception as oauth_err:
        logger.error(
            f"Email dispatch failed: {oauth_err}.\n"
            f"💡 TIP: For cloud/Render deployment, set GMAIL_APP_PASSWORD (16-digit Google App Password) "
            f"and SMTP_USER={ADMIN_EMAIL} in your Render environment variables."
        )
        raise oauth_err


# ===========================================================================
# HIGH-LEVEL CHECK FUNCTIONS
# ===========================================================================

def check_low_stock_for_product(product_id: str):
    """
    Single-product stock check triggered on product add/update or order placement.
    Always saves alert state to inventory_alerts table first, then sends email.
    """
    try:
        ensure_alerts_table()
        product = get_product_by_id(product_id)
        if not product:
            logger.warning(f"check_low_stock_for_product: {product_id} not found.")
            return

        stock = product.get("stock", 0)
        logger.info(
            f"Low-stock check | {product_id} stock={stock} threshold={LOW_STOCK_THRESHOLD}"
        )

        if stock > LOW_STOCK_THRESHOLD:
            reset_alert(product_id)
            return

        if is_alert_sent(product_id):
            logger.info(f"Alert already sent for {product_id} -- skipping duplicate.")
            return

        logger.info(
            f"LOW STOCK DETECTED: {product_id} ({product.get('product_name')}) stock={stock}"
        )

        # 1. ALWAYS persist alert in database first
        mark_alert_sent(product_id, stock)

        # 2. Attempt email notification
        try:
            email_content = generate_low_stock_email([product])
            send_email(email_content["subject"], email_content["body"])
        except Exception as email_err:
            logger.warning(f"Email sending failed for {product_id} (Alert is safely saved in DB): {email_err}")

    except Exception as exc:
        logger.error(f"check_low_stock_for_product({product_id}) error: {exc}")


def check_low_stock_products():
    """
    All-products stock check cycle.
    Persists alerts in database and sends a single consolidated email.
    """
    try:
        ensure_alerts_table()
        logger.info("Running full low-stock check cycle...")
        low_stock = get_low_stock_products()
        if not low_stock:
            logger.info("No low-stock products found.")
            return

        new_alerts = [p for p in low_stock if not is_alert_sent(p["product_id"])]
        if not new_alerts:
            logger.info(f"{len(low_stock)} low-stock product(s) in DB -- all already marked alerted.")
            return

        logger.info(f"New low stock items to alert: {[p['product_id'] for p in new_alerts]}")

        # 1. ALWAYS persist all new alerts in database first
        for p in new_alerts:
            mark_alert_sent(p["product_id"], p["stock"])

        # 2. Attempt consolidated email notification
        try:
            email_content = generate_low_stock_email(new_alerts)
            send_email(email_content["subject"], email_content["body"])
            logger.info(f"Consolidated alert email sent for {len(new_alerts)} product(s).")
        except Exception as email_err:
            logger.warning(f"Consolidated email sending failed (Alerts are safely saved in DB): {email_err}")

    except Exception as exc:
        logger.error(f"check_low_stock_products error: {exc}")


# ===========================================================================
# ASYNC BACKGROUND MONITOR
# ===========================================================================

async def run_stock_monitor():
    """
    Async background task started in FastAPI lifespan.
    Runs initial check immediately on startup, then every STOCK_CHECK_INTERVAL seconds.
    """
    logger.info(
        f"Stock monitor started | interval={STOCK_CHECK_INTERVAL}s | "
        f"threshold={LOW_STOCK_THRESHOLD} | admin={ADMIN_EMAIL}"
    )
    try:
        ensure_alerts_table()
        # Immediate initial check on startup (don't wait 5 minutes)
        await asyncio.to_thread(reset_restocked_alerts)
        await asyncio.to_thread(check_low_stock_products)
    except Exception as exc:
        logger.error(f"Initial stock check error: {exc}")

    while True:
        try:
            await asyncio.sleep(STOCK_CHECK_INTERVAL)
            await asyncio.to_thread(reset_restocked_alerts)
            await asyncio.to_thread(check_low_stock_products)
        except asyncio.CancelledError:
            logger.info("Stock monitor cancelled -- clean shutdown.")
            raise
        except Exception as exc:
            logger.error(f"Stock monitor cycle error (will retry): {exc}")


# ===========================================================================
# ONE-TIME SETUP CLI -- python email_automate.py
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("email_automate.py -- Low-Stock Alert System Test & Setup")
    print("=" * 60)
    print("\n[1] Ensuring inventory_alerts table...")
    ensure_alerts_table()
    print("    Done.")

    print("\n[2] Testing low-stock scan...")
    check_low_stock_products()

    print("\n[3] Testing email dispatch...")
    try:
        send_email(
            subject="[Test] ElectraStore Low-Stock System -- Test OK",
            body=(
                f"SmartStock Email System is configured correctly!\n\n"
                f"Admin email    : {ADMIN_EMAIL}\n"
                f"Threshold      : {LOW_STOCK_THRESHOLD} units\n"
                f"Check interval : {STOCK_CHECK_INTERVAL} seconds\n"
            ),
        )
        print(f"    Verification email sent successfully to {ADMIN_EMAIL}.")
    except Exception as e:
        print(f"    Email test failed: {e}")
        print("    Set GMAIL_APP_PASSWORD in .env for instant SMTP sending.")
