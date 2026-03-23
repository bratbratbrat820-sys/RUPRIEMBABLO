import os
import asyncio
import decimal
import asyncpg
import aiohttp
import uuid
from html import escape
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
from aiogram.fsm.storage.memory import MemoryStorage

try:
    from aiogram.client.default import DefaultBotProperties
    BOT_HAS_DEFAULTS = True
except Exception:
    BOT_HAS_DEFAULTS = False


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

UTC = timezone.utc
RUB_SIGN = "₽"
DIGITAL_CITY = "digital"

if BOT_HAS_DEFAULTS:
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
else:
    bot = Bot(BOT_TOKEN, parse_mode="HTML")

dp = Dispatcher(storage=MemoryStorage())
pool: asyncpg.Pool | None = None


# ================== TEXTS ==================
START_TEXT = f"""<b>{escape(SHOP_TITLE)}</b>

Премиальная витрина цифровых материалов с аккуратной выдачей и быстрым платежным сценарием.

• Только нужные разделы
• Каталог без мусора
• Оплата через PaySync
• Бронь товара на время оплаты
• Автоматическая проверка и выдача

Поддержка: @{escape(SUPPORT_USERNAME)}

Выбери действие ниже:"""

CATALOG_TEXT = """<b>Каталог</b>

Выбери нужный цифровой пакет:"""

ABOUT_TEXT = """<b>О магазине</b>

Здесь размещены цифровые пакеты, onboarding-материалы и рабочие наборы с удобной оплатой и быстрой выдачей после подтверждения платежа.
"""

SUPPORT_TEXT = f"""<b>Поддержка</b>

По вопросам оплаты, выдачи и заказа:
@{escape(SUPPORT_USERNAME)}"""

PROFILE_TEXT = """<b>Профиль</b>

ID: <code>{user_id}</code>
Заказов: <b>{orders}</b>
"""

ORDERS_EMPTY_TEXT = """<b>Мои заказы</b>

У тебя пока нет завершённых заказов."""

RESERVED_TEXT = """✅ Товар закреплён за тобой на <b>{minutes} минут</b>.

Теперь оплати заказ и потом нажми кнопку проверки платежа."""

PAYMENT_TEXT = """<b>Оплата через PaySync</b>

Заявка: <code>{trade_id}</code>
Карта: <code>{card}</code>
Сумма: <b>{amount} {currency}</b>
Срок оплаты: <b>{expires_at}</b>

Переводи ровно указанную сумму одним платежом.
После оплаты нажми кнопку проверки ниже.
"""

ALREADY_PAID_TEXT = "✅ Этот счёт уже был подтверждён ранее."
PAYMENT_WAIT_TEXT = "⏳ Оплата пока не подтверждена. Если ты уже оплатил — подожди немного и проверь ещё раз."
PAYMENT_EXPIRED_TEXT = "⏰ Время оплаты истекло. Создай заказ заново."
UNAVAILABLE_TEXT = "❌ Товар сейчас недоступен."
UNKNOWN_TEXT = "Используй кнопки ниже."


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
        price = decimal.Decimal(r["price"]).quantize(decimal.Decimal("0.01"))
        kb.append([
            InlineKeyboardButton(
                text=f"{r['name']} — {price:.2f} {RUB_SIGN}",
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


def back_home_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ На главную", callback_data="home")]
        ]
    )


# ================== HELPERS ==================
def utc_now() -> datetime:
    return datetime.now(UTC)


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def fmt_price(price: decimal.Decimal | int | float | str) -> str:
    return f"{decimal.Decimal(str(price)).quantize(decimal.Decimal('0.01')):.2f}"


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        await call.message.answer(text, reply_markup=reply_markup)


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

        # Сидинг безопасных цифровых пакетов без ломки существующей схемы
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
        "После подтверждения оплаты бот выдаст данные заказа в этом чате.",
        "Премиальный цифровой пакет по направлению Klein.\n\n• onboarding-материалы\n• аккуратная выдача\n• быстрый платёжный сценарий\n• поддержка по заказу"
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
        "После подтверждения оплаты бот выдаст данные заказа в этом чате.",
        "Цифровой пакет по направлению Tutti.\n\n• структурированная выдача\n• быстрый платёжный сценарий\n• понятный интерфейс\n• поддержка по заказу"
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
    return int(row["orders_count"]) if row else 0


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


async def get_user_orders_text(user_id: int) -> str:
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT item_name, price, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
            """,
            user_id
        )

    if not rows:
        return ORDERS_EMPTY_TEXT

    parts = ["<b>Мои заказы</b>\n"]
    for r in rows:
        created = r["created_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
        parts.append(
            f"• <b>{escape(str(r['item_name']))}</b>\n"
            f"  Сумма: <b>{fmt_price(r['price'])} {RUB_SIGN}</b>\n"
            f"  Дата: {created}\n"
        )
    return "\n".join(parts)


# ================== PaySync ==================
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
                raise RuntimeError(f"PaySync H2H bad response: {txt[:300]}")
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
    js = await paysync_h2h_create(logical_amount_int, PAYSYNC_CURRENCY, payload)

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
        raise RuntimeError("DB error: invoice not saved")
    return inv


async def invoice_apply_paid(trade_id: str) -> tuple[bool, str]:
    assert pool is not None

    async with pool.acquire() as con:
        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        return False, "❌ Заявка не найдена в базе."

    provider = str(inv["provider"] or "paysync")
    current_status = str(inv["status"] or "wait")
    kind = str(inv["kind"])
    user_id = int(inv["user_id"])
    product_code = inv["product_code"]

    if current_status in ("done", "paid"):
        return True, ALREADY_PAID_TEXT

    expires_at = inv["expires_at"]
    if expires_at and expires_at < utc_now():
        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
        if product_code:
            await release_product_reservation(str(product_code))
        return False, PAYMENT_EXPIRED_TEXT

    paid = False
    if provider == "paysync":
        js = await paysync_gettrans(trade_id)
        paid = (str(js.get("status") or "").lower() == "paid")
    else:
        return False, "❌ Неизвестный провайдер оплаты."

    if not paid:
        return False, PAYMENT_WAIT_TEXT

    assert pool is not None
    async with pool.acquire() as con:
        async with con.transaction():
            inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1 FOR UPDATE", trade_id)
            if not inv:
                return False, "❌ Заявка не найдена."

            if str(inv["status"] or "wait") in ("done", "paid"):
                return True, ALREADY_PAID_TEXT

            if kind != "product":
                await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                return True, "✅ Оплата подтверждена."

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
                    return True, "✅ Уже подтверждено ранее. Товар уже выдан."
                await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
                return True, "✅ Оплата подтверждена, но товар уже продан. Напиши оператору."

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
                return True, "✅ Оплата подтверждена, но данные ещё не добавлены. Напиши оператору."

            await con.execute(
                "UPDATE users SET orders_count = orders_count + 1 WHERE user_id=$1",
                user_id
            )
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
            await con.execute(
                "UPDATE invoices SET status='done', paid_at=NOW() WHERE trade_id=$1",
                trade_id
            )

            name = escape(str(product["name"]))
            return True, (
                f"✅ <b>Оплата подтверждена</b>\n\n"
                f"Покупка: <b>{name}</b>\n\n"
                f"📦 Выдача:\n{escape(link)}"
            )


# ================== COMMANDS / HANDLERS ==================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await ensure_user(message.from_user.id)
    await message.answer(START_TEXT, reply_markup=bottom_menu())
    await message.answer("Быстрые действия:", reply_markup=start_inline())


@dp.message(F.text == "🛍 Каталог")
async def catalog_msg(message: Message):
    await ensure_user(message.from_user.id)
    rows = await get_catalog_products()
    await message.answer(CATALOG_TEXT, reply_markup=catalog_inline(rows))


@dp.message(F.text == "📦 Мои заказы")
async def orders_msg(message: Message):
    await ensure_user(message.from_user.id)
    await message.answer(await get_user_orders_text(message.from_user.id), reply_markup=bottom_menu())


@dp.message(F.text == "👤 Профиль")
async def profile_msg(message: Message):
    await ensure_user(message.from_user.id)
    orders = await get_user_orders_count(message.from_user.id)
    await message.answer(
        PROFILE_TEXT.format(user_id=message.from_user.id, orders=orders),
        reply_markup=bottom_menu()
    )


@dp.message(F.text == "💬 Поддержка")
async def support_msg(message: Message):
    await message.answer(SUPPORT_TEXT, reply_markup=bottom_menu())


@dp.message(F.text == "ℹ️ О магазине")
async def about_msg(message: Message):
    await message.answer(ABOUT_TEXT, reply_markup=bottom_menu())


@dp.callback_query(F.data == "home")
async def home_cb(call: CallbackQuery):
    await safe_edit(call, START_TEXT, reply_markup=start_inline())
    await call.answer()


@dp.callback_query(F.data == "catalog")
async def catalog_cb(call: CallbackQuery):
    rows = await get_catalog_products()
    await safe_edit(call, CATALOG_TEXT, reply_markup=catalog_inline(rows))
    await call.answer()


@dp.callback_query(F.data == "orders")
async def orders_cb(call: CallbackQuery):
    text = await get_user_orders_text(call.from_user.id)
    await safe_edit(call, text, reply_markup=back_home_inline())
    await call.answer()


@dp.callback_query(F.data.startswith("product:"))
async def product_cb(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    product = await get_product(code)
    if not product or not product["is_active"] or product["sold_at"] is not None:
        await call.answer(UNAVAILABLE_TEXT, show_alert=True)
        return

    text = (
        f"<b>{escape(str(product['name']))}</b>\n\n"
        f"Цена: <b>{fmt_price(product['price'])} {RUB_SIGN}</b>\n\n"
        f"{escape(str(product['description'] or ''))}"
    )
    await safe_edit(call, text, reply_markup=product_inline(code))
    await call.answer()


@dp.callback_query(F.data.startswith("buy:"))
async def buy_cb(call: CallbackQuery):
    user_id = call.from_user.id
    product_code = call.data.split(":", 1)[1]

    await ensure_user(user_id)

    ok, msg = await reserve_product(user_id, product_code)
    if not ok:
        await call.answer(msg, show_alert=True)
        return

    product = await get_product(product_code)
    if not product:
        await call.answer(UNAVAILABLE_TEXT, show_alert=True)
        return

    price = decimal.Decimal(product["price"])
    logical_amount_int = price_to_int_rub(price)
    if logical_amount_int is None:
        await call.answer("❌ Цена товара должна быть целым числом в RUB.", show_alert=True)
        return

    try:
        inv = await create_product_invoice(user_id, product_code, logical_amount_int)
    except Exception as e:
        await release_product_reservation(product_code)
        await call.answer("❌ Не удалось создать счёт. Попробуй позже.", show_alert=True)
        print("[create_product_invoice]", e)
        return

    expires_at = inv["expires_at"]
    expires_text = expires_at.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC") if expires_at else "-"
    text = PAYMENT_TEXT.format(
        trade_id=escape(str(inv["trade_id"])),
        card=escape(str(inv["card_number"] or "-")),
        amount=str(inv["amount_int"]),
        currency=escape(str(inv["currency"])),
        expires_at=expires_text,
    )
    await safe_edit(call, text, reply_markup=check_payment_inline(str(inv["trade_id"])))
    await call.answer()


@dp.callback_query(F.data.startswith("check:"))
async def check_cb(call: CallbackQuery):
    trade_id = call.data.split(":", 1)[1]
    try:
        ok, text = await invoice_apply_paid(trade_id)
    except Exception as e:
        print("[invoice_apply_paid]", e)
        await call.answer("❌ Ошибка проверки оплаты. Попробуй ещё раз.", show_alert=True)
        return

    if ok:
        await safe_edit(call, text, reply_markup=back_home_inline())
        await call.answer("✅ Готово", show_alert=False)
    else:
        await call.answer(text, show_alert=True)


# ================== ADMIN ==================
@dp.message(F.text.startswith("/addproduct"))
async def addproduct_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    # /addproduct code | name | price | link | description
    raw = message.text[len("/addproduct"):].strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) != 5:
        await message.answer(
            "Формат:\n"
            "/addproduct code | name | price_rub | link | description"
        )
        return

    code, name, price_raw, link, description = parts

    try:
        price = decimal.Decimal(price_raw).quantize(decimal.Decimal("0.01"))
    except Exception:
        await message.answer("❌ Неверная цена.")
        return

    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO products(code, city, name, price, link, description, is_active)
            VALUES($1,$2,$3,$4,$5,$6,TRUE)
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
            code, DIGITAL_CITY, name, price, link, description
        )
    await message.answer("✅ Товар сохранён.")


@dp.message(F.text.startswith("/deactivate"))
async def deactivate_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /deactivate code")
        return

    code = parts[1].strip()
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "UPDATE products SET is_active=FALSE WHERE code=$1",
            code
        )
    await message.answer("✅ Товар отключён.")


@dp.message()
async def unknown_msg(message: Message):
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
