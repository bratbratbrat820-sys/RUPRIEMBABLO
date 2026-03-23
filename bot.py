import os
import asyncio
import decimal
import asyncpg
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.storage.memory import MemoryStorage

# ================== ENV ==================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
ADMIN_ID_RAW = (os.getenv("ADMIN_ID") or "").strip()

PAYSYNC_APIKEY = (os.getenv("PAYSYNC_APIKEY") or "").strip()
PAYSYNC_CLIENT_ID = (os.getenv("PAYSYNC_CLIENT_ID") or "").strip()
PAYSYNC_CURRENCY = (os.getenv("PAYSYNC_CURRENCY") or "RUB").strip().upper()

PAYMENT_TIMEOUT_MINUTES_RAW = (os.getenv("PAYMENT_TIMEOUT_MINUTES") or "15").strip()
RESERVATION_MINUTES_RAW = (os.getenv("RESERVATION_MINUTES") or "15").strip()

SHOP_TITLE = (os.getenv("SHOP_TITLE") or "Premium Digital Store").strip()
SUPPORT_USERNAME = (os.getenv("SUPPORT_USERNAME") or "your_support").strip().lstrip("@")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")
if not ADMIN_ID_RAW or not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("ADMIN_ID is missing or invalid")
if not PAYSYNC_APIKEY:
    raise RuntimeError("PAYSYNC_APIKEY is missing")
if not PAYSYNC_CLIENT_ID or not PAYSYNC_CLIENT_ID.isdigit():
    raise RuntimeError("PAYSYNC_CLIENT_ID is missing or invalid")

ADMIN_ID = int(ADMIN_ID_RAW)
CLIENT_ID = int(PAYSYNC_CLIENT_ID)

try:
    PAYMENT_TIMEOUT_MINUTES = max(1, int(PAYMENT_TIMEOUT_MINUTES_RAW))
except Exception:
    PAYMENT_TIMEOUT_MINUTES = 15

try:
    RESERVATION_MINUTES = max(1, int(RESERVATION_MINUTES_RAW))
except Exception:
    RESERVATION_MINUTES = 15

RUB = "₽"
DIGITAL_CITY = "digital"

# ================== TEXTS ==================
START_TEXT = f"""<b>{SHOP_TITLE}</b>

Премиальная витрина цифровых продуктов с быстрой оплатой и моментальной обработкой.

• Быстрый каталог
• Чистый интерфейс
• Без лишних разделов
• Оплата через PaySync
• Поддержка по заказу и выдаче

Поддержка: @{SUPPORT_USERNAME}

Выбери действие ниже:"""

CATALOG_TEXT = """<b>Каталог</b>

Выбери нужный продукт:"""

ABOUT_TEXT = """<b>О магазине</b>

Здесь размещены цифровые пакеты и материалы с удобной оплатой и выдачей после подтверждения платежа.

После оплаты бот фиксирует заказ и выдаёт данные по покупке автоматически.
"""

SUPPORT_TEXT = f"""<b>Поддержка</b>

По вопросам оплаты, заказа и выдачи:
@{SUPPORT_USERNAME}
"""

PROFILE_TEXT = """<b>Профиль</b>

ID: <code>{user_id}</code>
Заказов: <b>{orders}</b>
"""

ORDERS_EMPTY_TEXT = """<b>Мои заказы</b>

У тебя пока нет завершённых заказов.
"""

ITEM_TEXT = """<b>{name}</b>

Цена: <b>{price} {rub}</b>

{desc}
"""

RESERVED_TEXT = """✅ Товар временно закреплён за тобой на <b>{minutes} минут</b>.

Теперь оплати заказ и после этого нажми кнопку проверки платежа.
"""

PAYMENT_TEXT = """<b>Оплата через PaySync</b>

Заявка: <code>{trade_id}</code>
Карта: <code>{card}</code>
Сумма: <b>{amount} {currency}</b>
Срок оплаты: <b>{expires_at}</b>

Переводи ровно указанную сумму одним платежом.
После оплаты нажми кнопку проверки ниже.
"""

PAID_TEXT = """✅ <b>Оплата подтверждена</b>

Товар: <b>{name}</b>

Твоя выдача:
{delivery}
"""

ALREADY_PAID_TEXT = "✅ Этот счёт уже был подтверждён ранее."
PAYMENT_WAIT_TEXT = "⏳ Оплата пока не подтверждена. Если ты уже оплатил — подожди немного и проверь ещё раз."
PAYMENT_EXPIRED_TEXT = "⏰ Время оплаты истекло. Создай заказ заново."
UNAVAILABLE_TEXT = "❌ Товар сейчас недоступен."

# ================== KEYBOARDS ==================
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Каталог"), KeyboardButton(text="📦 Мои заказы")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="💬 Поддержка")],
            [KeyboardButton(text="ℹ️ О магазине")],
        ],
        resize_keyboard=True,
    )


def start_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Открыть каталог", callback_data="catalog")],
            [InlineKeyboardButton(text="📦 Мои заказы", callback_data="orders")],
            [InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}")],
        ]
    )


def catalog_inline(rows: list[asyncpg.Record]) -> InlineKeyboardMarkup:
    kb = []
    for r in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{r['name']} — {decimal.Decimal(r['price']):.0f} {RUB}",
                callback_data=f"product:{r['code']}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def product_inline(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Купить сейчас", callback_data=f"buy:{code}")],
            [InlineKeyboardButton(text="⬅ К каталогу", callback_data="catalog")],
        ]
    )


def check_payment_inline(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check:{trade_id}")],
            [InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog")],
        ]
    )

# ================== DB ==================
pool: asyncpg.Pool | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


async def db_init() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance NUMERIC(12,2) NOT NULL DEFAULT 0,
            orders_count INT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS products (
            code TEXT PRIMARY KEY,
            city TEXT NOT NULL,
            name TEXT NOT NULL,
            price NUMERIC(12,2) NOT NULL DEFAULT 0,
            link TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS product_code TEXT")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS item_name TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS price NUMERIC(12,2) NOT NULL DEFAULT 0")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS link TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'paysync'")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS external_payment_id TEXT NOT NULL DEFAULT ''")

        await con.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            trade_id TEXT PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            amount_int INT NOT NULL DEFAULT 0,
            amount INT NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'RUB',
            product_code TEXT,
            card_number TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'wait',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS amount_int INT NOT NULL DEFAULT 0")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS amount INT NOT NULL DEFAULT 0")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS product_code TEXT")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS card_number TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'wait'")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'RUB'")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'paysync'")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_id TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS pay_url TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payload TEXT NOT NULL DEFAULT ''")
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ")

        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS reserved_by BIGINT")
        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS reserved_until TIMESTAMPTZ")
        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sold_at TIMESTAMPTZ")
        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sold_to BIGINT")

        # Сидинг двух товаров без ломки существующей схемы
        await con.execute("""
        INSERT INTO products(code, city, name, price, link, description, is_active)
        VALUES($1,$2,$3,$4,$5,$6,TRUE)
        ON CONFLICT (code) DO UPDATE SET
            city=EXCLUDED.city,
            name=EXCLUDED.name,
            price=EXCLUDED.price,
            description=EXCLUDED.description,
            is_active=TRUE
        """,
        "klein_pack",
        DIGITAL_CITY,
        "Klein Digital Pack",
        decimal.Decimal("5490.00"),
        "После оплаты бот выдаст данные заказа здесь.",
        "Премиальный цифровой пакет по категории Klein.\n\n• удобная выдача\n• быстрая обработка\n• чистый формат заказа\n• поддержка по заказу"
        )

        await con.execute("""
        INSERT INTO products(code, city, name, price, link, description, is_active)
        VALUES($1,$2,$3,$4,$5,$6,TRUE)
        ON CONFLICT (code) DO UPDATE SET
            city=EXCLUDED.city,
            name=EXCLUDED.name,
            price=EXCLUDED.price,
            description=EXCLUDED.description,
            is_active=TRUE
        """,
        "tutti_pack",
        DIGITAL_CITY,
        "Tutti Digital Pack",
        decimal.Decimal("5990.00"),
        "После оплаты бот выдаст данные заказа здесь.",
        "Цифровой пакет по категории Tutti.\n\n• аккуратная выдача\n• быстрый платёжный сценарий\n• понятный интерфейс\n• поддержка по заказу"
        )


async def ensure_user(user_id: int) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING",
            user_id,
        )


async def get_user_orders_count(user_id: int) -> int:
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT orders_count FROM users WHERE user_id=$1", user_id)
    if not row:
        return 0
    return int(row["orders_count"])


async def get_catalog_products() -> list[asyncpg.Record]:
    assert pool is not None
    async with pool.acquire() as con:
        return await con.fetch(
            """
            SELECT code, name, price
            FROM products
            WHERE city=$1
              AND is_active=TRUE
              AND sold_at IS NULL
              AND (reserved_until IS NULL OR reserved_until < NOW())
            ORDER BY created_at DESC
            """,
            DIGITAL_CITY
        )


async def get_product(code: str) -> asyncpg.Record | None:
    assert pool is not None
    async with pool.acquire() as con:
        return await con.fetchrow(
            """
            SELECT code, city, name, price, link, description, is_active,
                   reserved_by, reserved_until, sold_at, sold_to
            FROM products
            WHERE code=$1
            """,
            code
        )


async def reserve_product(user_id: int, product_code: str) -> tuple[bool, str]:
    assert pool is not None
    until = utc_now() + timedelta(minutes=RESERVATION_MINUTES)

    async with pool.acquire() as con:
        async with con.transaction():
            row = await con.fetchrow(
                """
                SELECT code, is_active, reserved_by, reserved_until, sold_at
                FROM products
                WHERE code=$1
                FOR UPDATE
                """,
                product_code
            )

            if not row:
                return False, UNAVAILABLE_TEXT
            if not row["is_active"] or row["sold_at"] is not None:
                return False, UNAVAILABLE_TEXT

            reserved_by = row["reserved_by"]
            reserved_until = row["reserved_until"]

            if reserved_until and reserved_until < utc_now():
                reserved_by = None
                reserved_until = None

            if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != user_id:
                return False, "❌ Этот товар сейчас временно забронирован другим клиентом."

            await con.execute(
                """
                UPDATE products
                SET reserved_by=$2, reserved_until=$3
                WHERE code=$1
                """,
                product_code, user_id, until
            )

    return True, RESERVED_TEXT.format(minutes=RESERVATION_MINUTES)


async def release_product_reservation(product_code: str) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            UPDATE products
            SET reserved_by=NULL, reserved_until=NULL
            WHERE code=$1 AND sold_at IS NULL
            """,
            product_code
        )


async def cleanup_expired() -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE products
            SET reserved_by=NULL, reserved_until=NULL
            WHERE sold_at IS NULL
              AND reserved_until IS NOT NULL
              AND reserved_until < NOW()
        """)
        await con.execute("""
            UPDATE invoices
            SET status='expired'
            WHERE status='wait'
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
        """)


async def cleanup_loop():
    while True:
        try:
            await cleanup_expired()
        except Exception as e:
            print("[cleanup]", e)
        await asyncio.sleep(30)


# ================== PAYSYNC ==================
async def paysync_create_invoice(amount_int: int, currency: str, data: str) -> dict:
    url = f"https://paysync.bot/api/client{CLIENT_ID}/amount{amount_int}/currency{currency}"
    params = {"data": data}
    headers = {"Content-Type": "application/json", "apikey": PAYSYNC_APIKEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers, timeout=30) as resp:
            try:
                js = await resp.json()
            except Exception:
                txt = await resp.text()
                raise RuntimeError(f"PaySync bad response: {txt[:300]}")
    return js


async def paysync_gettrans(trade_id: str) -> dict:
    url = f"https://paysync.bot/gettrans/{trade_id}"
    headers = {"Content-Type": "application/json", "apikey": PAYSYNC_APIKEY}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            try:
                return await resp.json()
            except Exception:
                txt = await resp.text()
                raise RuntimeError(f"PaySync gettrans bad response: {txt[:300]}")


def safe_int_from_paysync_amount(val) -> int | None:
    try:
        d = decimal.Decimal(str(val).replace(",", ".").strip())
        d2 = d.quantize(decimal.Decimal("1"))
        if d2 <= 0:
            return None
        return int(d2)
    except Exception:
        return None


def price_to_int_rub(price: decimal.Decimal) -> int | None:
    p = price.quantize(decimal.Decimal("0.01"))
    if p != p.quantize(decimal.Decimal("1.00")):
        return None
    return int(p)


async def create_product_invoice(user_id: int, product_code: str, logical_amount_int: int) -> asyncpg.Record:
    payload = f"product:{user_id}:{product_code}:{uuid.uuid4().hex[:10]}"
    js = await paysync_create_invoice(logical_amount_int, PAYSYNC_CURRENCY, payload)

    trade = js.get("trade")
    card_number = js.get("card_number") or ""
    status = (js.get("status") or "wait").lower()
    currency = js.get("currency") or PAYSYNC_CURRENCY

    if not trade:
        raise RuntimeError(f"PaySync create missing trade: {js}")

    amount_to_pay_int = safe_int_from_paysync_amount(js.get("amount"))
    if amount_to_pay_int is None:
        amount_to_pay_int = logical_amount_int

    trade_id = str(trade)
    expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO invoices(
                trade_id, user_id, kind, amount_int, amount, currency,
                product_code, card_number, status, provider, external_id,
                pay_url, expires_at, payload
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (trade_id) DO UPDATE SET
              user_id=EXCLUDED.user_id,
              kind=EXCLUDED.kind,
              amount_int=EXCLUDED.amount_int,
              amount=EXCLUDED.amount,
              currency=EXCLUDED.currency,
              product_code=EXCLUDED.product_code,
              card_number=EXCLUDED.card_number,
              status=EXCLUDED.status,
              provider=EXCLUDED.provider,
              external_id=EXCLUDED.external_id,
              pay_url=EXCLUDED.pay_url,
              expires_at=EXCLUDED.expires_at,
              payload=EXCLUDED.payload
            """,
            trade_id, user_id, "product",
            amount_to_pay_int,
            logical_amount_int,
            str(currency),
            product_code,
            str(card_number),
            str(status),
            "paysync",
            trade_id,
            "",
            expires_at,
            payload,
        )

        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        raise RuntimeError("Invoice save error")
    return inv


async def apply_paid_invoice(trade_id: str) -> tuple[bool, str]:
    assert pool is not None

    async with pool.acquire() as con:
        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        return False, "❌ Счёт не найден."

    status_now = str(inv["status"] or "wait")
    if status_now in ("done", "paid"):
        return True, ALREADY_PAID_TEXT

    if status_now == "expired":
        return False, PAYMENT_EXPIRED_TEXT

    expires_at = inv["expires_at"]
    if expires_at and expires_at < utc_now():
        async with pool.acquire() as con:
            await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
        if inv["product_code"]:
            await release_product_reservation(str(inv["product_code"]))
        return False, PAYMENT_EXPIRED_TEXT

    js = await paysync_gettrans(trade_id)
    ps_status = (js.get("status") or "").lower()
    if ps_status != "paid":
        return False, PAYMENT_WAIT_TEXT

    user_id = int(inv["user_id"])
    product_code = str(inv["product_code"] or "")

    async with pool.acquire() as con:
        async with con.transaction():
            product = await con.fetchrow(
                """
                SELECT code, name, price, link, is_active, sold_at, sold_to, reserved_by, reserved_until
                FROM products
                WHERE code=$1
                FOR UPDATE
                """,
                product_code
            )
