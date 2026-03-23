import os
import asyncio
import decimal
import asyncpg
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

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
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

try:
    from aiogram.types import CopyTextButton  # type: ignore
    HAS_COPY_TEXT_BUTTON = True
except Exception:
    CopyTextButton = None  # type: ignore
    HAS_COPY_TEXT_BUTTON = False


# ================== ENV ==================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
ADMIN_ID_RAW = (os.getenv("ADMIN_ID") or "").strip()

PAYSYNC_APIKEY = (os.getenv("PAYSYNC_APIKEY") or "").strip()
PAYSYNC_CLIENT_ID = (os.getenv("PAYSYNC_CLIENT_ID") or "").strip()
PAYSYNC_CURRENCY = (os.getenv("PAYSYNC_CURRENCY") or "RUB").strip().upper()

CRYPTO_PAY_API_TOKEN = (os.getenv("CRYPTO_PAY_API_TOKEN") or "").strip()
CRYPTO_PAY_BASE_URL = (os.getenv("CRYPTO_PAY_BASE_URL") or "https://pay.crypt.bot/api").strip().rstrip("/")
CRYPTO_PAY_FIAT = (os.getenv("CRYPTO_PAY_FIAT") or "RUB").strip().upper()
CRYPTO_PAY_ACCEPTED_ASSETS = (os.getenv("CRYPTO_PAY_ACCEPTED_ASSETS") or "USDT,TON,BTC,ETH,LTC,BNB,TRX,USDC").strip()

PAYMENT_TIMEOUT_MINUTES_RAW = (os.getenv("PAYMENT_TIMEOUT_MINUTES") or "15").strip()
RESERVATION_MINUTES_RAW = (os.getenv("RESERVATION_MINUTES") or "15").strip()

SUPPORT_USERNAME = (os.getenv("SUPPORT_USERNAME") or "potterspotter").strip().lstrip("@")
SHOP_TITLE = (os.getenv("SHOP_TITLE") or "ATELIER").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")
if not ADMIN_ID_RAW or not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("ADMIN_ID is missing or invalid")
if not PAYSYNC_APIKEY:
    raise RuntimeError("PAYSYNC_APIKEY is missing")
if not PAYSYNC_CLIENT_ID or not PAYSYNC_CLIENT_ID.isdigit():
    raise RuntimeError("PAYSYNC_CLIENT_ID is missing or invalid (must be digits)")

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
UTC = timezone.utc
DIGITAL_CITY = "digital"

bot = Bot(BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())
pool: asyncpg.Pool | None = None


# ================== FSM ==================
class PromoStates(StatesGroup):
    waiting_code = State()


class TopupStates(StatesGroup):
    waiting_amount = State()


# ================== TEXTS ==================
START_TEXT = f"""<b>{SHOP_TITLE}</b>

Закрытая витрина цифровых позиций с чистым интерфейсом и быстрой обработкой платежей.

• минималистичный каталог
• моментальная бронь на оплату
• пополнение баланса
• история и промокоды
• поддержка по заказу

Поддержка: @{SUPPORT_USERNAME}
"""

ABOUT_TEXT = """<b>О витрине</b>

Лаконичный магазин цифровых позиций с оплатой через PaySync и выдачей после подтверждения платежа.

Всё оформлено без лишнего шума: каталог, профиль, история, пополнение и поддержка.
"""

SUPPORT_TEXT = f"""<b>Поддержка</b>

По оплате, выдаче и любым вопросам:
@{SUPPORT_USERNAME}"""

PROFILE_TEXT = """<b>Профиль</b>

Баланс: <b>{balance:.2f} {rub}</b>
Заказов: <b>{orders}</b>

Выбери действие ниже.
"""

CATALOG_TEXT = """<b>Каталог</b>

Выбери позицию:"""

ORDERS_EMPTY_TEXT = """<b>Мои заказы</b>

Пока пусто."""

PROMO_ASK_TEXT = """<b>Промокод</b>

Отправь код одним сообщением."""
TOPUP_ASK_TEXT = """<b>Пополнение</b>

Введи сумму целым числом, например: <code>5000</code>"""

ITEM_TEXT = """<b>{name}</b>

Цена: <b>{price} {rub}</b>

{desc}"""

RESERVED_TEXT = """✅ Позиция закреплена за тобой на <b>{minutes} минут</b>.

Оплати вовремя и потом нажми проверку оплаты."""

ALREADY_PAID_TEXT = "✅ Этот счёт уже был подтверждён ранее."
PAYMENT_WAIT_TEXT = "⏳ Оплата пока не подтверждена. Если перевод уже отправлен — подожди немного и проверь ещё раз."
PAYMENT_EXPIRED_TEXT = "⏰ Время оплаты истекло. Создай новый заказ."
UNAVAILABLE_TEXT = "❌ Позиция сейчас недоступна."

PAYSYNC_PAYMENT_TEXT = """<b>Оплата через PaySync</b>

Заявка: <code>{trade_id}</code>
Карта: <code>{card}</code>
Сумма: <b>{amount} {currency}</b>
Срок оплаты: <b>{expires_at}</b>

На оплату даётся 15 минут.

Оплачивай одним переводом и точно в указанной сумме.
Если платёж не проходит или есть тех. вопрос — поддержка: @PaySyncSupportBot
Основная поддержка магазина: @{support}

После перевода нажми кнопку проверки ниже.
"""

CRYPTO_PAYMENT_TEXT = """<b>CryptoBot invoice</b>

Заявка: <code>{trade_id}</code>
Сумма: <b>{amount} {currency}</b>

Перейди по кнопке ниже, оплати счёт и затем нажми проверку.
"""

PAID_PRODUCT_TEXT = """✅ <b>Оплата подтверждена</b>

Позиция: <b>{name}</b>

Твоя выдача:
{delivery}
"""

PAID_TOPUP_TEXT = "✅ Оплата подтверждена.\nБаланс пополнен на <b>{amount}</b>."
HISTORY_HEADER = "<b>История покупок</b>\n"
BALANCE_HEADER = "<b>Пополнение</b>\n\nВыбери способ:"


# ================== KEYBOARDS ==================
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="ОТКРЫТЬ КАТАЛОГ")],
            [KeyboardButton(text="ПРОФИЛЬ"), KeyboardButton(text="ПОДДЕРЖКА")],
            [KeyboardButton(text="О ВИТРИНЕ")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выбери раздел",
    )


def inline_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Каталог", callback_data="catalog:open")],
            [InlineKeyboardButton(text="Профиль", callback_data="profile:open")],
        ]
    )


def inline_catalog(rows: list[asyncpg.Record]) -> InlineKeyboardMarkup:
    kb = []
    for r in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{r['name']} — {decimal.Decimal(r['price']):.0f} {RUB}",
                callback_data=f"product:{r['code']}"
            )
        ])
    kb.append([InlineKeyboardButton(text="На главную", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_product(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Купить", callback_data=f"buy:{code}")],
            [InlineKeyboardButton(text="К каталогу", callback_data="catalog:open")],
        ]
    )


def inline_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Пополнить", callback_data="profile:topup"),
                InlineKeyboardButton(text="История", callback_data="profile:history"),
            ],
            [
                InlineKeyboardButton(text="Промокод", callback_data="profile:promo"),
                InlineKeyboardButton(text="Заказы", callback_data="profile:orders"),
            ],
            [InlineKeyboardButton(text="На главную", callback_data="home")],
        ]
    )


def inline_topup_methods() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="PaySync", callback_data="topup_method:paysync")]]
    if CRYPTO_PAY_API_TOKEN:
        rows.append([InlineKeyboardButton(text="CryptoBot", callback_data="topup_method:crypto")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="profile:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_amounts(provider: str) -> InlineKeyboardMarkup:
    amounts = [3000, 5000, 7000, 10000]
    rows = [[InlineKeyboardButton(text=f"{a} {RUB}", callback_data=f"topup_amount:{provider}:{a}")] for a in amounts]
    rows.append([InlineKeyboardButton(text="Ввести свою сумму", callback_data=f"topup_custom:{provider}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="profile:topup")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_check_only(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{trade_id}")],
            [InlineKeyboardButton(text="Каталог", callback_data="catalog:open")],
        ]
    )


def inline_check_and_copy(trade_id: str, card_number: str | None) -> InlineKeyboardMarkup:
    rows = []
    if HAS_COPY_TEXT_BUTTON and card_number:
        rows.append([InlineKeyboardButton(text="Скопировать карту", copy_text=CopyTextButton(text=card_number))])  # type: ignore
    rows.append([InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{trade_id}")])
    rows.append([InlineKeyboardButton(text="Каталог", callback_data="catalog:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_crypto_pay(url: str, trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Оплатить счёт", url=url)],
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{trade_id}")],
            [InlineKeyboardButton(text="Каталог", callback_data="catalog:open")],
        ]
    )


# ================== UTILS ==================
def utc_now() -> datetime:
    return datetime.now(UTC)


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def parse_int_amount(s: str) -> int | None:
    try:
        s = s.strip().replace(" ", "").replace(",", ".")
        d = decimal.Decimal(s)
        if d != d.quantize(decimal.Decimal("1")):
            return None
        i = int(d)
        if i <= 0:
            return None
        return i
    except Exception:
        return None


def safe_dt_to_text(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")


def safe_int_from_paysync_amount(val) -> int | None:
    try:
        d = decimal.Decimal(str(val).replace(",", ".").strip())
        d2 = d.quantize(decimal.Decimal("1"))
        if d2 <= 0:
            return None
        return int(d2)
    except Exception:
        return None


def render_h2h_message(inv: asyncpg.Record) -> str:
    card = str(inv["card_number"] or "").strip() or "—"
    return PAYSYNC_PAYMENT_TEXT.format(
        trade_id=str(inv["trade_id"]),
        card=card,
        amount=int(inv["amount_int"]),
        currency=str(inv["currency"]),
        expires_at=safe_dt_to_text(inv["expires_at"]),
        support=SUPPORT_USERNAME,
    )


def render_crypto_message(inv: asyncpg.Record) -> str:
    return CRYPTO_PAYMENT_TEXT.format(
        trade_id=str(inv["trade_id"]),
        amount=int(inv["amount"]),
        currency=str(inv["currency"]),
    )


# ================== DB ==================
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

        await con.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount NUMERIC(12,2) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            uses_left INT NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS promo_activations (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            code TEXT NOT NULL REFERENCES promo_codes(code) ON DELETE CASCADE,
            activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, code)
        )
        """)

        # Сидим безопасные цифровые позиции
        await con.execute("""
        INSERT INTO products(code, city, name, price, link, description, is_active)
        VALUES($1,$2,$3,$4,$5,$6,TRUE)
        ON CONFLICT (code) DO UPDATE SET
            city=EXCLUDED.city,
            name=EXCLUDED.name,
            price=EXCLUDED.price,
            link=EXCLUDED.link,
            description=EXCLUDED.description,
            is_active=TRUE
        """, "klein_private", DIGITAL_CITY, "Klein Private Pack", decimal.Decimal("5290.00"),
        "Выдача приходит в этот чат после подтверждения оплаты.",
        "Лаконичная цифровая позиция в линейке Klein.\n\n• быстрый платёжный сценарий\n• минималистичная выдача\n• ручная поддержка при необходимости")

        await con.execute("""
        INSERT INTO products(code, city, name, price, link, description, is_active)
        VALUES($1,$2,$3,$4,$5,$6,TRUE)
        ON CONFLICT (code) DO UPDATE SET
            city=EXCLUDED.city,
            name=EXCLUDED.name,
            price=EXCLUDED.price,
            link=EXCLUDED.link,
            description=EXCLUDED.description,
            is_active=TRUE
        """, "tutti_private", DIGITAL_CITY, "Tutti Private Pack", decimal.Decimal("5790.00"),
        "Выдача приходит в этот чат после подтверждения оплаты.",
        "Цифровая позиция направления Tutti.\n\n• премиальный вид\n• чистая подача\n• быстрая проверка оплаты")

        await con.execute("""
        INSERT INTO products(code, city, name, price, link, description, is_active)
        VALUES($1,$2,$3,$4,$5,$6,TRUE)
        ON CONFLICT (code) DO UPDATE SET
            city=EXCLUDED.city,
            name=EXCLUDED.name,
            price=EXCLUDED.price,
            link=EXCLUDED.link,
            description=EXCLUDED.description,
            is_active=TRUE
        """, "ricardo_private", DIGITAL_CITY, "Ricardo Private Pack", decimal.Decimal("5990.00"),
        "Выдача приходит в этот чат после подтверждения оплаты.",
        "Линейка Ricardo в аккуратной витрине.\n\n• короткий путь до оплаты\n• бронь на 15 минут\n• спокойный интерфейс")


async def ensure_user(user_id: int) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING",
            user_id,
        )


async def get_profile(user_id: int) -> asyncpg.Record:
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT balance, orders_count FROM users WHERE user_id=$1", user_id)
    return row


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
                return False, "❌ Эта позиция временно занята другим клиентом."

            await con.execute(
                "UPDATE products SET reserved_by=$2, reserved_until=$3 WHERE code=$1",
                product_code, user_id, until
            )
    return True, RESERVED_TEXT.format(minutes=RESERVATION_MINUTES)


async def release_product_reservation(product_code: str) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "UPDATE products SET reserved_by=NULL, reserved_until=NULL WHERE code=$1 AND sold_at IS NULL",
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


async def get_history(user_id: int) -> list[asyncpg.Record]:
    assert pool is not None
    async with pool.acquire() as con:
        return await con.fetch(
            """
            SELECT item_name, price, link, provider, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
            """,
            user_id
        )


async def activate_promo(user_id: int, code_raw: str) -> tuple[bool, str]:
    code = (code_raw or "").strip().upper()
    if not code:
        return False, "❌ Пустой промокод."

    assert pool is not None
    async with pool.acquire() as con:
        async with con.transaction():
            promo = await con.fetchrow(
                "SELECT code, amount, is_active, uses_left FROM promo_codes WHERE code=$1 FOR UPDATE",
                code
            )
            if not promo:
                return False, "❌ Промокод не найден."
            if not promo["is_active"]:
                return False, "❌ Промокод отключён."
            if int(promo["uses_left"]) <= 0:
                return False, "❌ У промокода закончились активации."

            already = await con.fetchrow(
                "SELECT id FROM promo_activations WHERE user_id=$1 AND code=$2",
                user_id, code
            )
            if already:
                return False, "❌ Ты уже активировал этот промокод."

            amount = decimal.Decimal(promo["amount"]).quantize(decimal.Decimal("0.01"))

            await con.execute(
                "INSERT INTO promo_activations(user_id, code) VALUES($1, $2)",
                user_id, code
            )
            await con.execute(
                "UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=$1",
                code
            )
            await con.execute(
                "UPDATE users SET balance = balance + $2 WHERE user_id=$1",
                user_id, amount
            )

    return True, f"✅ Промокод активирован.\nБаланс пополнен на <b>{amount:.2f} {RUB}</b>."


# ================== CRYPTO ==================
async def crypto_create_invoice(user_id: int, logical_amount_int: int, kind: str, product_code: str | None, description: str) -> asyncpg.Record:
    if not CRYPTO_PAY_API_TOKEN:
        raise RuntimeError("Crypto token is not configured")

    payload = {
        "asset": "USDT",
        "fiat": CRYPTO_PAY_FIAT,
        "amount": str(logical_amount_int),
        "description": description,
        "accepted_assets": CRYPTO_PAY_ACCEPTED_ASSETS,
    }
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN,
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{CRYPTO_PAY_BASE_URL}/createInvoice",
            json=payload,
            headers=headers,
            timeout=30,
        ) as resp:
            js = await resp.json(content_type=None)

    if not js.get("ok"):
        raise RuntimeError(str(js))

    invoice = js["result"]
    ext_id = str(invoice["invoice_id"])
    trade_id = f"crypto_{ext_id}"
    pay_url = str(invoice.get("bot_invoice_url") or "")
    expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO invoices(
                trade_id, user_id, kind, amount_int, amount, currency, product_code,
                card_number, status, provider, external_id, pay_url, expires_at, payload
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,'','wait','crypto',$8,$9,$10,$11)
            ON CONFLICT (trade_id) DO UPDATE SET
                status='wait',
                external_id=EXCLUDED.external_id,
                pay_url=EXCLUDED.pay_url,
                expires_at=EXCLUDED.expires_at,
                payload=EXCLUDED.payload
            """,
            trade_id, user_id, kind, logical_amount_int, logical_amount_int,
            CRYPTO_PAY_FIAT, product_code, ext_id, pay_url, expires_at, description
        )
        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)
    return inv


async def crypto_get_invoice(external_id: str) -> dict | None:
    if not CRYPTO_PAY_API_TOKEN:
        return None
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{CRYPTO_PAY_BASE_URL}/getInvoices?invoice_ids={external_id}",
            headers=headers,
            timeout=30,
        ) as resp:
            js = await resp.json(content_type=None)

    if not js.get("ok"):
        return None
    items = js.get("result", {}).get("items", [])
    return items[0] if items else None


# ================== PAYSYNC ==================
async def paysync_h2h_create(amount_int: int, currency: str, data: str) -> dict:
    data_q = quote(data or "")
    url = f"https://paysync.bot/api/client{CLIENT_ID}/amount{amount_int}/currency{currency}?data={data_q}"
    headers = {"Content-Type": "application/json", "apikey": PAYSYNC_APIKEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
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


async def invoice_create_paysync(user_id: int, kind: str, logical_amount_int: int, product_code: str | None) -> asyncpg.Record:
    payload = f"{kind}:{user_id}:{product_code or '-'}:{uuid.uuid4().hex[:10]}"
    js = await paysync_h2h_create(logical_amount_int, PAYSYNC_CURRENCY, payload)

    trade_id = str(js.get("trade") or "")
    if not trade_id:
        raise RuntimeError(f"PaySync create missing trade: {js}")

    card_number = str(js.get("card_number") or js.get("card") or "")
    status = str(js.get("status") or "wait").lower()
    amount_to_pay_int = safe_int_from_paysync_amount(js.get("amount"))
    if amount_to_pay_int is None:
        amount_to_pay_int = logical_amount_int

    expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO invoices(
                trade_id, user_id, kind, amount_int, amount, currency,
                product_code, card_number, status, provider, external_id,
                expires_at, payload
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'paysync',$10,$11,$12)
            ON CONFLICT (trade_id) DO UPDATE SET
                status=EXCLUDED.status,
                card_number=EXCLUDED.card_number,
                amount_int=EXCLUDED.amount_int,
                expires_at=EXCLUDED.expires_at,
                payload=EXCLUDED.payload
            """,
            trade_id, user_id, kind, amount_to_pay_int, logical_amount_int,
            PAYSYNC_CURRENCY, product_code, card_number, status, trade_id, expires_at, payload
        )
        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)
    return inv


async def invoice_apply_paid(trade_id: str) -> tuple[bool, str]:
    assert pool is not None
    async with pool.acquire() as con:
        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        return False, "❌ Счёт не найден."

    current_status = str(inv["status"] or "wait")
    provider = str(inv["provider"] or "paysync")
    kind = str(inv["kind"])
    user_id = int(inv["user_id"])
    product_code = inv["product_code"]

    if current_status in ("done", "paid"):
        return True, ALREADY_PAID_TEXT

    expires_at = inv["expires_at"]
    if expires_at and expires_at < utc_now():
        async with pool.acquire() as con:
            await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
        if product_code:
            await release_product_reservation(str(product_code))
        return False, PAYMENT_EXPIRED_TEXT

    if provider == "paysync":
        js = await paysync_gettrans(trade_id)
        status = str(js.get("status") or "").lower()
        if status != "paid":
            return False, PAYMENT_WAIT_TEXT

    elif provider == "crypto":
        external_id = str(inv["external_id"] or "").strip()
        if not external_id:
            return False, "❌ Не найден crypto invoice."
        crypto_invoice = await crypto_get_invoice(external_id)
        if not crypto_invoice:
            return False, "❌ Не удалось получить crypto invoice."
        crypto_status = str(crypto_invoice.get("status") or "").lower()
        if crypto_status == "expired":
            async with pool.acquire() as con:
                await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
            if product_code:
                await release_product_reservation(str(product_code))
            return False, PAYMENT_EXPIRED_TEXT
        if crypto_status != "paid":
            return False, PAYMENT_WAIT_TEXT

    else:
        return False, "❌ Неизвестный провайдер оплаты."

    logical_amount_int = int(inv["amount"])

    if kind == "topup":
        add_sum = decimal.Decimal(logical_amount_int).quantize(decimal.Decimal("0.01"))
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute("UPDATE users SET balance = balance + $2 WHERE user_id=$1", user_id, add_sum)
                await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
        return True, PAID_TOPUP_TEXT.format(amount=f"{logical_amount_int} {RUB}")

    if kind == "product":
        if not product_code:
            async with pool.acquire() as con:
                await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
            return True, "✅ Оплата подтверждена, но товар не привязан. Напиши оператору."

        async with pool.acquire() as con:
            async with con.transaction():
                product = await con.fetchrow(
                    """
                    SELECT code, name, price, link, is_active, sold_at, sold_to, reserved_by, reserved_until
                    FROM products
                    WHERE code=$1
                    FOR UPDATE
                    """,
                    str(product_code)
                )

                if not product:
                    await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                    return True, "✅ Оплата подтверждена, но товар не найден. Напиши оператору."

                if product["sold_at"] is not None:
                    if product["sold_to"] == user_id:
                        await con.execute("UPDATE invoices SET status='done', paid_at=NOW() WHERE trade_id=$1", trade_id)
                        return True, "✅ Уже подтверждено ранее. Позиция уже выдана."
                    await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                    return True, "✅ Оплата подтверждена, но позиция уже продана. Напиши оператору."

                reserved_by = product["reserved_by"]
                reserved_until = product["reserved_until"]
                if reserved_until and reserved_until < utc_now():
                    reserved_by = None
                    reserved_until = None

                if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != user_id:
                    await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                    return True, "✅ Оплата подтверждена, но бронь уже занята другим пользователем. Напиши оператору."

                link = str(product["link"] or "").strip()
                if not link:
                    await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                    return True, "✅ Оплата подтверждена, но выдача пока не добавлена. Напиши оператору."

                await con.execute("UPDATE users SET orders_count = orders_count + 1 WHERE user_id=$1", user_id)
                await con.execute(
                    """
                    INSERT INTO purchases(user_id, product_code, item_name, price, link, provider, external_payment_id)
                    VALUES($1,$2,$3,$4,$5,$6,$7)
                    """,
                    user_id,
                    str(product_code),
                    str(product["name"]),
                    decimal.Decimal(product["price"]),
                    link,
                    provider,
                    str(inv["external_id"] or trade_id),
                )
                await con.execute(
                    """
                    UPDATE products
                    SET is_active=FALSE,
                        sold_at=NOW(),
                        sold_to=$2,
                        reserved_by=NULL,
                        reserved_until=NULL
                    WHERE code=$1
                    """,
                    str(product_code), user_id
                )
                await con.execute("UPDATE invoices SET status='done', paid_at=NOW() WHERE trade_id=$1", trade_id)

                return True, PAID_PRODUCT_TEXT.format(name=product["name"], delivery=link)

    return False, "❌ Неизвестный тип заявки."


# ================== RENDER ==================
async def render_profile(message_or_call, user_id: int):
    await ensure_user(user_id)
    prof = await get_profile(user_id)
    text = PROFILE_TEXT.format(
        balance=decimal.Decimal(prof["balance"]),
        rub=RUB,
        orders=int(prof["orders_count"]),
    )
    if isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.answer(text, reply_markup=inline_profile())
    else:
        await message_or_call.answer(text, reply_markup=inline_profile())


async def render_catalog_message(message_or_call):
    rows = await get_catalog_products()
    if isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.answer(CATALOG_TEXT, reply_markup=inline_catalog(rows))
    else:
        await message_or_call.answer(CATALOG_TEXT, reply_markup=inline_catalog(rows))


# ================== HANDLERS ==================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await ensure_user(message.from_user.id)
    await message.answer(START_TEXT, reply_markup=bottom_menu())
    await message.answer("Выбери раздел:", reply_markup=inline_home())


@dp.message(F.text == "ОТКРЫТЬ КАТАЛОГ")
async def msg_catalog(message: Message):
    await render_catalog_message(message)


@dp.message(F.text == "ПРОФИЛЬ")
async def msg_profile(message: Message):
    await render_profile(message, message.from_user.id)


@dp.message(F.text == "ПОДДЕРЖКА")
async def msg_support(message: Message):
    await message.answer(SUPPORT_TEXT, reply_markup=bottom_menu())


@dp.message(F.text == "О ВИТРИНЕ")
async def msg_about(message: Message):
    await message.answer(ABOUT_TEXT, reply_markup=bottom_menu())


@dp.callback_query(F.data == "home")
async def cb_home(call: CallbackQuery):
    await call.answer()
    await call.message.answer(START_TEXT, reply_markup=inline_home())


@dp.callback_query(F.data == "catalog:open")
async def cb_catalog(call: CallbackQuery):
    await call.answer()
    await render_catalog_message(call)


@dp.callback_query(F.data == "profile:open")
async def cb_profile_open(call: CallbackQuery):
    await call.answer()
    await render_profile(call, call.from_user.id)


@dp.callback_query(F.data == "profile:orders")
async def cb_profile_orders(call: CallbackQuery):
    await call.answer()
    rows = await get_history(call.from_user.id)
    if not rows:
        await call.message.answer(ORDERS_EMPTY_TEXT)
        return

    text = "<b>Мои заказы</b>\n\n"
    for r in rows:
        dt = r["created_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
        price = decimal.Decimal(r["price"])
        text += f"• <b>{r['item_name']}</b>\n{price:.2f} {RUB} • {dt}\n\n"
    await call.message.answer(text, reply_markup=inline_profile())


@dp.callback_query(F.data == "profile:history")
async def cb_profile_history(call: CallbackQuery):
    await call.answer()
    rows = await get_history(call.from_user.id)
    if not rows:
        await call.message.answer(ORDERS_EMPTY_TEXT)
        return

    text = HISTORY_HEADER + "\n"
    for r in rows:
        dt = r["created_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
        price = decimal.Decimal(r["price"])
        provider = str(r["provider"] or "unknown")
        text += f"• {r['item_name']} — {price:.2f} {RUB} [{provider}] ({dt})\n{r['link']}\n\n"
    await call.message.answer(text, reply_markup=inline_profile())


@dp.callback_query(F.data == "profile:promo")
async def cb_profile_promo(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await state.set_state(PromoStates.waiting_code)
    await call.message.answer(PROMO_ASK_TEXT)


@dp.message(PromoStates.waiting_code)
async def promo_entered(message: Message, state: FSMContext):
    await ensure_user(message.from_user.id)
    ok, msg = await activate_promo(message.from_user.id, message.text or "")
    await message.answer(msg, reply_markup=inline_profile())
    await state.clear()


@dp.callback_query(F.data == "profile:topup")
async def cb_profile_topup(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await call.message.answer(BALANCE_HEADER, reply_markup=inline_topup_methods())


@dp.callback_query(F.data.startswith("topup_method:"))
async def cb_topup_method(call: CallbackQuery, state: FSMContext):
    await call.answer()
    provider = call.data.split(":", 1)[1]
    await state.clear()
    await state.update_data(topup_provider=provider)
    await call.message.answer("Выбери сумму:", reply_markup=inline_amounts(provider))


@dp.callback_query(F.data.startswith("topup_amount:"))
async def cb_topup_amount(call: CallbackQuery, state: FSMContext):
    await call.answer()
    _, provider, amount_s = call.data.split(":")
    logical_amount_int = int(amount_s)
    await create_topup_invoice(call.message, call.from_user.id, provider, logical_amount_int)
    await state.clear()


@dp.callback_query(F.data.startswith("topup_custom:"))
async def cb_topup_custom(call: CallbackQuery, state: FSMContext):
    await call.answer()
    provider = call.data.split(":", 1)[1]
    await state.set_state(TopupStates.waiting_amount)
    await state.update_data(topup_provider=provider)
    await call.message.answer(TOPUP_ASK_TEXT)


@dp.message(TopupStates.waiting_amount)
async def topup_amount_entered(message: Message, state: FSMContext):
    logical_amount_int = parse_int_amount(message.text or "")
    if logical_amount_int is None:
        await message.answer("❌ Введи сумму целым числом. Пример: 5000")
        return
    if logical_amount_int < 100:
        await message.answer(f"❌ Минимум 100 {RUB}.")
        return

    data = await state.get_data()
    provider = str(data.get("topup_provider") or "paysync")
    await create_topup_invoice(message, message.from_user.id, provider, logical_amount_int)
    await state.clear()


async def create_topup_invoice(target_message: Message, user_id: int, provider: str, logical_amount_int: int):
    try:
        if provider == "crypto":
            inv = await crypto_create_invoice(
                user_id,
                logical_amount_int,
                "topup",
                None,
                f"Пополнение баланса | {logical_amount_int} {CRYPTO_PAY_FIAT}",
            )
            await target_message.answer(
                render_crypto_message(inv),
                reply_markup=inline_crypto_pay(str(inv["pay_url"]), str(inv["trade_id"]))
            )
        else:
            inv = await invoice_create_paysync(user_id, "topup", logical_amount_int, None)
            await target_message.answer(
                render_h2h_message(inv),
                reply_markup=inline_check_and_copy(str(inv["trade_id"]), str(inv["card_number"] or "").strip() or None),
            )
    except Exception as e:
        await target_message.answer(f"❌ Ошибка создания оплаты: {e}")


@dp.callback_query(F.data.startswith("product:"))
async def cb_product(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":", 1)[1]
    product = await get_product(code)
    if not product or not product["is_active"] or product["sold_at"] is not None:
        await call.message.answer(UNAVAILABLE_TEXT)
        return

    text = ITEM_TEXT.format(
        name=product["name"],
        price=f"{decimal.Decimal(product['price']):.0f}",
        rub=RUB,
        desc=product["description"],
    )
    await call.message.answer(text, reply_markup=inline_product(code))


@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id
    product_code = call.data.split(":", 1)[1]

    await ensure_user(user_id)

    ok, reserve_msg = await reserve_product(user_id, product_code)
    if not ok:
        await call.message.answer(reserve_msg)
        return

    product = await get_product(product_code)
    if not product:
        await call.message.answer(UNAVAILABLE_TEXT)
        return

    price = decimal.Decimal(product["price"]).quantize(decimal.Decimal("1.00"))
    logical_amount_int = int(price)

    try:
        inv = await invoice_create_paysync(user_id, "product", logical_amount_int, product_code)
        await call.message.answer(RESERVED_TEXT.format(minutes=RESERVATION_MINUTES))
        await call.message.answer(
            render_h2h_message(inv),
            reply_markup=inline_check_and_copy(str(inv["trade_id"]), str(inv["card_number"] or "").strip() or None),
        )
    except Exception as e:
        await release_product_reservation(product_code)
        await call.message.answer(f"❌ Не удалось создать счёт: {e}")


@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]
    try:
        ok, msg = await invoice_apply_paid(trade_id)
    except Exception as e:
        await call.message.answer(f"❌ Ошибка проверки оплаты: {e}")
        return
    await call.message.answer(msg, reply_markup=inline_profile())


# ================== ADMIN ==================
@dp.message(F.text.startswith("/addproduct"))
async def cmd_addproduct(message: Message):
    if not is_admin(message.from_user.id):
        return

    raw = message.text.strip()
    parts = [p.strip() for p in raw[len("/addproduct"):].strip().split("|")]
    if len(parts) < 5:
        await message.answer("Формат:\n/addproduct code | name | price | link | desc")
        return

    code = parts[0]
    name = parts[1]
    price = decimal.Decimal(parts[2].replace(",", "."))
    link = parts[3]
    desc = parts[4]

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO products(code, city, name, price, link, description, is_active, sold_at, sold_to, reserved_by, reserved_until)
            VALUES($1,$2,$3,$4,$5,$6,TRUE,NULL,NULL,NULL,NULL)
            ON CONFLICT (code) DO UPDATE SET
                city=EXCLUDED.city,
                name=EXCLUDED.name,
                price=EXCLUDED.price,
                link=EXCLUDED.link,
                description=EXCLUDED.description,
                is_active=TRUE,
                sold_at=NULL,
                sold_to=NULL,
                reserved_by=NULL,
                reserved_until=NULL
            """,
            code, DIGITAL_CITY, name, price, link, desc
        )
    await message.answer("✅ Позиция сохранена.")


@dp.message(F.text.startswith("/promo"))
async def cmd_promo(message: Message):
    if not is_admin(message.from_user.id):
        return
    # /promo CODE | 500 | 1
    raw = message.text[len("/promo"):].strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) != 3:
        await message.answer("Формат:\n/promo CODE | amount | uses")
        return

    code = parts[0].upper()
    amount = decimal.Decimal(parts[1].replace(",", ".")).quantize(decimal.Decimal("0.01"))
    uses = int(parts[2])

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO promo_codes(code, amount, is_active, uses_left)
            VALUES($1,$2,TRUE,$3)
            ON CONFLICT (code) DO UPDATE SET
                amount=EXCLUDED.amount,
                is_active=TRUE,
                uses_left=EXCLUDED.uses_left
            """,
            code, amount, uses
        )
    await message.answer("✅ Промокод сохранён.")


@dp.message()
async def fallback(message: Message):
    await message.answer(UNKNOWN_TEXT, reply_markup=bottom_menu())


# ================== MAIN ==================
async def main():
    await db_init()
    asyncio.create_task(cleanup_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
