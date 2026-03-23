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

# Опционально: если установлен aiogram с поддержкой CopyTextButton,
# появится inline-кнопка "Скопировать карту"
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
PAYSYNC_CURRENCY = (os.getenv("PAYSYNC_CURRENCY") or "UAH").strip().upper()

CRYPTO_PAY_API_TOKEN = (os.getenv("CRYPTO_PAY_API_TOKEN") or "").strip()
CRYPTO_PAY_BASE_URL = (os.getenv("CRYPTO_PAY_BASE_URL") or "https://pay.crypt.bot/api").strip().rstrip("/")
CRYPTO_PAY_FIAT = (os.getenv("CRYPTO_PAY_FIAT") or "UAH").strip().upper()
CRYPTO_PAY_ACCEPTED_ASSETS = (os.getenv("CRYPTO_PAY_ACCEPTED_ASSETS") or "USDT,TON,BTC,ETH,LTC,BNB,TRX,USDC").strip()

PAYMENT_TIMEOUT_MINUTES_RAW = (os.getenv("PAYMENT_TIMEOUT_MINUTES") or "15").strip()
RESERVATION_MINUTES_RAW = (os.getenv("RESERVATION_MINUTES") or "15").strip()

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

UAH = "₴"


# ================== TEXTS ==================
MAIN_TEXT_TEMPLATE = """Приветствуем Кавалер 🫡

✍🏻О СЕРВИСЕ

°Готовые Товары 💪🏻
°ОПТ ⭕️
°Шустрые смены сортов 💨
°Разновидные способы оплаты 🌐
°Отправки NovaPost 🇺🇦
°Оператор/Сапорт в сети 24/7 🟢

Актуальная Информация 

Бот :
@CavalierShopBot

Оператор/Сапорт :
@Cavalerskiy_supp

🏦Баланс : {balance} {uah}
🛍️Количество заказов : {orders}
"""

PROFILE_TEXT_TEMPLATE = """👤 Профиль

🏦Баланс : {balance} {uah}
🛍️Количество заказов : {orders}
"""

HELP_TEXT = """По Случаю НеНахода/Имеющихся вопросов, писать :
@Cavalerskiy_supp
"""

WORK_TEXT = "Ищем ответственных сотрудников магазина, подробности @Cavalerskiy_supp"

ITEM_TEXT_TEMPLATE = """✅ Вы выбрали: {name}

Цена: {price} {uah}

{desc}
"""

DISTRICT_TEXT = "📍 Выберите способ оплаты:"
TOPUP_ASK_TEXT = f"💳 Введите сумму пополнения в гривнах ({UAH}) целым числом:\nНапример: 150"


# ================== KEYBOARDS ==================
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="ГЛАВНАЯ 🔘"), KeyboardButton(text="ПРОФИЛЬ 👤")],
            [KeyboardButton(text="ПОМОЩЬ 💬"), KeyboardButton(text="РАБОТА 💸")],
        ],
        resize_keyboard=True,
    )


def inline_main_city() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Одесса", callback_data="city:odesa")]]
    )


def inline_one_button(text: str, cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=cb)]]
    )


def inline_pay_buttons(product_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Балансом", callback_data=f"pay:bal:{product_code}")],
            [InlineKeyboardButton(text="Картой (PaySync)", callback_data=f"pay:card:{product_code}")],
            [InlineKeyboardButton(text="Crypto", callback_data=f"pay:crypto:{product_code}")],
        ]
    )


def inline_profile_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить баланс", callback_data="profile:topup")],
            [InlineKeyboardButton(text="🎟 Активировать промокод", callback_data="profile:promo")],
            [InlineKeyboardButton(text="🧾 История покупок", callback_data="profile:history")],
        ]
    )


def inline_topup_methods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="PaySync", callback_data="topup_method:paysync")],
            [InlineKeyboardButton(text="Crypto", callback_data="topup_method:crypto")],
        ]
    )


def inline_check_only(invoice_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check:{invoice_key}")]]
    )


def inline_check_and_copy(invoice_key: str, card_number: str | None = None) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check:{invoice_key}")]]
    if card_number and HAS_COPY_TEXT_BUTTON:
        rows.append([
            InlineKeyboardButton(
                text="📋 Скопировать карту",
                copy_text=CopyTextButton(text=card_number)
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ================== DB ==================
pool: asyncpg.Pool | None = None


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def normalize_code(raw: str) -> str:
    return (raw or "").strip()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_int_amount(text: str) -> int | None:
    try:
        s = (text or "").strip().replace(",", ".")
        d = decimal.Decimal(s)
        if d <= 0:
            return None
        d2 = d.quantize(decimal.Decimal("1"))
        if d2 != d:
            return None
        return int(d2)
    except Exception:
        return None


def price_to_int_uah(price: decimal.Decimal) -> int | None:
    p = price.quantize(decimal.Decimal("0.01"))
    if p != p.quantize(decimal.Decimal("1.00")):
        return None
    return int(p)


def safe_int_from_paysync_amount(val) -> int | None:
    try:
        d = decimal.Decimal(str(val).replace(",", ".").strip())
        d2 = d.quantize(decimal.Decimal("1"))
        if d2 <= 0:
            return None
        return int(d2)
    except Exception:
        return None


def safe_dt_to_text(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


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
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'balance'")
        await con.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS external_payment_id TEXT NOT NULL DEFAULT ''")

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
            code TEXT NOT NULL REFERENCES promo_codes(code),
            activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, code)
        )
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            trade_id TEXT PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            amount_int INT NOT NULL DEFAULT 0,
            amount INT NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'UAH',
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
        await con.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'UAH'")
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


async def ensure_user(user_id: int) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING",
            user_id,
        )


async def get_user_stats(user_id: int) -> tuple[decimal.Decimal, int]:
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT balance, orders_count FROM users WHERE user_id=$1",
            user_id,
        )
    if not row:
        return decimal.Decimal("0.00"), 0
    return decimal.Decimal(row["balance"]), int(row["orders_count"])


async def render_main_text(user_id: int) -> str:
    await ensure_user(user_id)
    bal, orders = await get_user_stats(user_id)
    return MAIN_TEXT_TEMPLATE.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)


async def cleanup_expired_reservations() -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE products
            SET reserved_by = NULL,
                reserved_until = NULL
            WHERE sold_at IS NULL
              AND reserved_until IS NOT NULL
              AND reserved_until < NOW()
        """)
        await con.execute("""
            UPDATE invoices
            SET status = 'expired'
            WHERE status = 'wait'
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
        """)


async def background_cleanup_loop():
    while True:
        try:
            await cleanup_expired_reservations()
        except Exception as e:
            print(f"[cleanup] {e}")
        await asyncio.sleep(30)


async def get_city_products(city: str, limit: int = 20) -> list[asyncpg.Record]:
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
            LIMIT $2
            """,
            city, limit
        )


def inline_city_products(rows: list[asyncpg.Record], city: str) -> InlineKeyboardMarkup:
    if not rows:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Нет товаров", callback_data="noop")]]
        )
    kb = []
    for r in rows:
        name = str(r["name"])
        code = str(r["code"])
        price = decimal.Decimal(r["price"])
        kb.append([InlineKeyboardButton(text=f"{name} — {price:.2f} {UAH}", callback_data=f"prod:{city}:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


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


async def add_or_update_product(city: str, code: str, name: str, price: decimal.Decimal, link: str, desc: str) -> None:
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
                reserved_by=NULL,
                reserved_until=NULL,
                sold_at=NULL,
                sold_to=NULL
            """,
            code, city, name, price, link, desc
        )


async def deactivate_product(code: str) -> bool:
    assert pool is not None
    async with pool.acquire() as con:
        res = await con.execute("UPDATE products SET is_active=FALSE WHERE code=$1", code)
    return res.startswith("UPDATE")


async def get_history(user_id: int) -> list[asyncpg.Record]:
    assert pool is not None
    async with pool.acquire() as con:
        return await con.fetch(
            """
            SELECT item_name, link, price, provider, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
            """,
            user_id,
        )


async def activate_promo(user_id: int, raw_code: str) -> tuple[bool, str]:
    code = normalize_code(raw_code)
    if not code:
        return False, "❌ Введи промокод текстом."

    assert pool is not None
    async with pool.acquire() as con:
        async with con.transaction():
            promo = await con.fetchrow(
                """
                SELECT code, amount, is_active, uses_left
                FROM promo_codes
                WHERE upper(code) = upper($1)
                FOR UPDATE
                """,
                code,
            )

            if not promo or not promo["is_active"] or int(promo["uses_left"]) <= 0:
                return False, "❌ Промокод недействителен."

            real_code = str(promo["code"])
            amount = decimal.Decimal(promo["amount"])

            used = await con.fetchval(
                "SELECT 1 FROM promo_activations WHERE user_id=$1 AND code=$2",
                user_id, real_code
            )
            if used:
                return False, "❌ Ты уже активировал этот промокод."

            await con.execute(
                "INSERT INTO promo_activations(user_id, code) VALUES($1, $2)",
                user_id, real_code
            )
            await con.execute(
                "UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=$1",
                real_code
            )
            await con.execute(
                "UPDATE users SET balance = balance + $2 WHERE user_id=$1",
                user_id, amount
            )

    return True, f"✅ Промокод активирован!\n🏦 Начислено: {amount:.2f} {UAH}"


# ================== RESERVATION ==================
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
                return False, "❌ Товар не найден."
            if not row["is_active"] or row["sold_at"] is not None:
                return False, "❌ Товар недоступен."

            reserved_by = row["reserved_by"]
            reserved_until = row["reserved_until"]

            if reserved_until and reserved_until < utc_now():
                reserved_by = None
                reserved_until = None

            if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != user_id:
                return False, "❌ Этот товар сейчас временно забронирован другим покупателем."

            await con.execute(
                """
                UPDATE products
                SET reserved_by=$2, reserved_until=$3
                WHERE code=$1
                """,
                product_code, user_id, until
            )

    return True, f"✅ Товар забронирован за тобой на {RESERVATION_MINUTES} минут."


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


async def cancel_waiting_invoices_for_product(product_code: str) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            """
            UPDATE invoices
            SET status='cancelled'
            WHERE product_code=$1 AND status='wait'
            """,
            product_code
        )


async def cancel_waiting_invoice(trade_id: str) -> tuple[bool, str]:
    assert pool is not None
    async with pool.acquire() as con:
        async with con.transaction():
            inv = await con.fetchrow(
                "SELECT trade_id, product_code, status FROM invoices WHERE trade_id=$1 FOR UPDATE",
                trade_id
            )
            if not inv:
                return False, "❌ Счёт не найден."
            if str(inv["status"]) not in ("wait", "expired"):
                return False, f"❌ Этот счёт нельзя отменить в статусе: {inv['status']}"

            await con.execute(
                "UPDATE invoices SET status='cancelled' WHERE trade_id=$1",
                trade_id
            )
            if inv["product_code"]:
                await con.execute(
                    """
                    UPDATE products
                    SET reserved_by=NULL, reserved_until=NULL
                    WHERE code=$1 AND sold_at IS NULL
                    """,
                    str(inv["product_code"])
                )

    return True, "✅ Счёт отменён, бронь снята."


# ================== BUY WITH BALANCE ==================
async def buy_with_balance(user_id: int, product_code: str) -> tuple[bool, str]:
    await ensure_user(user_id)
    assert pool is not None

    async with pool.acquire() as con:
        async with con.transaction():
            product = await con.fetchrow(
                """
                SELECT code, name, price, link, is_active, sold_at, reserved_by, reserved_until
                FROM products
                WHERE code=$1
                FOR UPDATE
                """,
                product_code
            )

            if not product or not product["is_active"] or product["sold_at"] is not None:
                return False, "❌ Товар недоступен."

            reserved_by = product["reserved_by"]
            reserved_until = product["reserved_until"]
            if reserved_until and reserved_until < utc_now():
                reserved_by = None
                reserved_until = None

            if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != user_id:
                return False, "❌ Товар временно забронирован другим покупателем."

            price = decimal.Decimal(product["price"])
            name = str(product["name"])
            link = str(product["link"] or "").strip()
            if not link:
                return False, "❌ Для этого товара ещё не добавлена ссылка."

            row = await con.fetchrow(
                "SELECT balance, orders_count FROM users WHERE user_id=$1 FOR UPDATE",
                user_id
            )
            bal = decimal.Decimal(row["balance"])
            if bal < price:
                return False, f"❌ Недостаточно средств.\nНужно: {price:.2f} {UAH}\nУ тебя: {bal:.2f} {UAH}"

            await con.execute(
                "UPDATE users SET balance = balance - $2, orders_count = orders_count + 1 WHERE user_id=$1",
                user_id, price
            )

            await con.execute(
                """
                INSERT INTO purchases(user_id, product_code, item_name, price, link, provider, external_payment_id)
                VALUES($1,$2,$3,$4,$5,$6,$7)
                """,
                user_id, product_code, name, price, link, "balance", ""
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
                product_code, user_id
            )

            await con.execute(
                """
                UPDATE invoices
                SET status='done'
                WHERE product_code=$1 AND status IN ('wait', 'paid')
                """,
                product_code
            )

    return True, f"✅ Покупка успешна: {name}\nСписано: {price:.2f} {UAH}\n\n🔗 Твоя ссылка:\n{link}"


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


# ================== CRYPTO PAY ==================
async def crypto_api_request(method: str, payload: dict | None = None) -> dict:
    if not CRYPTO_PAY_API_TOKEN:
        raise RuntimeError("CRYPTO_PAY_API_TOKEN is missing")

    url = f"{CRYPTO_PAY_BASE_URL}/{method}"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=(payload or {}), timeout=30) as resp:
            try:
                js = await resp.json()
            except Exception:
                txt = await resp.text()
                raise RuntimeError(f"Crypto Pay bad response: {txt[:300]}")

    if not js.get("ok"):
        raise RuntimeError(f"Crypto Pay error: {js.get('error', 'unknown error')}")
    return js


async def crypto_create_invoice(amount_int: int, title: str, payload_text: str) -> dict:
    body = {
        "currency_type": "fiat",
        "fiat": CRYPTO_PAY_FIAT,
        "accepted_assets": CRYPTO_PAY_ACCEPTED_ASSETS,
        "amount": f"{amount_int:.2f}",
        "description": title[:1024],
        "payload": payload_text[:4096],
        "allow_comments": False,
        "allow_anonymous": True,
        "expires_in": PAYMENT_TIMEOUT_MINUTES * 60,
    }
    js = await crypto_api_request("createInvoice", body)
    return js["result"]


async def crypto_get_invoice(invoice_id: str) -> dict | None:
    js = await crypto_api_request("getInvoices", {"invoice_ids": str(invoice_id)})
    result = js.get("result")
    if isinstance(result, dict):
        items = result.get("items", [])
    else:
        items = result or []
    if not items:
        return None
    return items[0]


# ================== INVOICES ==================
async def invoice_create_paysync(user_id: int, kind: str, logical_amount_int: int, product_code: str | None) -> asyncpg.Record:
    await ensure_user(user_id)

    nonce = uuid.uuid4().hex[:10]
    data = f"{kind}:{user_id}:{product_code or '-'}:{nonce}"

    js = await paysync_h2h_create(logical_amount_int, PAYSYNC_CURRENCY, data)

    trade = js.get("trade")
    card_number = js.get("card_number") or ""
    status = (js.get("status") or "wait").lower()
    currency = js.get("currency") or PAYSYNC_CURRENCY

    if not trade:
        raise RuntimeError(f"PaySync create missing 'trade': {js}")

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
            trade_id, user_id, kind,
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
            data,
        )

        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        raise RuntimeError("DB error: invoice not saved")
    return inv


async def invoice_create_crypto(user_id: int, logical_amount_int: int, kind: str, product_code: str | None, title: str) -> asyncpg.Record:
    await ensure_user(user_id)

    nonce = uuid.uuid4().hex[:10]
    payload_text = f"{kind}:{user_id}:{product_code or '-'}:{nonce}"

    js = await crypto_create_invoice(logical_amount_int, title, payload_text)

    invoice_id = str(js.get("invoice_id") or "").strip()
    pay_url = str(js.get("bot_invoice_url") or "").strip()
    status = str(js.get("status") or "active").lower()
    fiat = str(js.get("fiat") or CRYPTO_PAY_FIAT)
    expiration_date_raw = js.get("expiration_date")

    if not invoice_id:
        raise RuntimeError("Crypto Pay не вернул invoice_id")
    if not pay_url:
        raise RuntimeError("Crypto Pay не вернул bot_invoice_url")

    expires_at = None
    if expiration_date_raw:
        try:
            expires_at = datetime.fromisoformat(str(expiration_date_raw).replace("Z", "+00:00"))
        except Exception:
            expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)
    else:
        expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    trade_id = f"crypto_{invoice_id}"

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
            trade_id,
            user_id,
            kind,
            logical_amount_int,
            logical_amount_int,
            fiat,
            product_code,
            "",
            "wait" if status == "active" else status,
            "crypto",
            invoice_id,
            pay_url,
            expires_at,
            payload_text,
        )

        inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

    if not inv:
        raise RuntimeError("DB error: crypto invoice not saved")
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
        if kind == "topup":
            return True, "✅ Уже подтверждено ранее. Баланс пополнен."
        return True, "✅ Уже подтверждено ранее. Товар уже выдан."

    if current_status == "cancelled":
        return False, "❌ Этот счёт уже отменён."

    expires_at = inv["expires_at"]
    if expires_at and expires_at < utc_now():
        async with pool.acquire() as con:
            await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
        if product_code:
            await release_product_reservation(str(product_code))
        return False, "❌ Время оплаты истекло."

    if provider == "paysync":
        js = await paysync_gettrans(trade_id)
        status = (js.get("status") or "").lower()
        if status != "paid":
            return False, "❌ Оплата ещё не подтверждена."

    elif provider == "crypto":
        external_id = str(inv["external_id"] or "").strip()
        if not external_id:
            return False, "❌ Не найден внешний ID crypto-счёта."

        crypto_invoice = await crypto_get_invoice(external_id)
        if not crypto_invoice:
            return False, "❌ Не удалось получить crypto invoice."

        crypto_status = str(crypto_invoice.get("status") or "").lower()
        if crypto_status == "expired":
            async with pool.acquire() as con:
                await con.execute("UPDATE invoices SET status='expired' WHERE trade_id=$1", trade_id)
            if product_code:
                await release_product_reservation(str(product_code))
            return False, "❌ Время оплаты истекло."
        if crypto_status != "paid":
            return False, "❌ Оплата ещё не подтверждена."

    else:
        return False, "❌ Неизвестный провайдер оплаты."

    logical_amount_int = int(inv["amount"])

    if kind == "topup":
        add_sum = decimal.Decimal(logical_amount_int).quantize(decimal.Decimal("0.01"))
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "UPDATE users SET balance = balance + $2 WHERE user_id=$1",
                    user_id, add_sum
                )
                await con.execute(
                    "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                    trade_id
                )

        return True, f"✅ Оплата подтверждена.\n🏦 Баланс пополнен на {logical_amount_int} {UAH}"

    if kind == "product":
        if not product_code:
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                    trade_id
                )
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
                    await con.execute(
                        "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                        trade_id
                    )
                    return True, "✅ Оплата подтверждена, но товар не найден. Напиши оператору."

                if product["sold_at"] is not None:
                    if product["sold_to"] == user_id:
                        await con.execute(
                            "UPDATE invoices SET status='done', paid_at=NOW() WHERE trade_id=$1",
                            trade_id
                        )
                        return True, "✅ Уже подтверждено ранее. Товар уже выдан."
                    await con.execute(
                        "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                        trade_id
                    )
                    return True, "✅ Оплата подтверждена, но товар уже продан. Напиши оператору."

                reserved_by = product["reserved_by"]
                reserved_until = product["reserved_until"]
                if reserved_until and reserved_until < utc_now():
                    reserved_by = None
                    reserved_until = None

                if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != user_id:
                    await con.execute(
                        "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                        trade_id
                    )
                    return True, "✅ Оплата подтверждена, но бронь уже занята другим пользователем. Напиши оператору."

                link = str(product["link"] or "").strip()
                if not link:
                    await con.execute(
                        "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                        trade_id
                    )
                    return True, "✅ Оплата подтверждена, но ссылка на товар не добавлена. Напиши оператору."

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

                return True, f"✅ Оплата подтверждена.\n✅ Покупка успешна: {product['name']}\n\n🔗 Твоя ссылка:\n{link}"

    return False, "❌ Неизвестный тип заявки."


def render_h2h_message(inv: asyncpg.Record) -> str:
    trade_id = str(inv["trade_id"])
    amount_to_pay_int = int(inv["amount_int"])
    currency = str(inv["currency"])
    card = str(inv["card_number"] or "").strip()
    expires_at = inv["expires_at"]

    if not card:
        card = "—"

    return (
        f"💳 Оплата через PaySync\n\n"
        f"🧾 Заявка: {trade_id}\n"
        f"💳 Карта для оплаты:\n`{card}`\n"
        f"💰 Сумма: {amount_to_pay_int} {currency}\n"
        f"⏳ Срок оплаты: до {safe_dt_to_text(expires_at)}\n\n"
        f"Оплачивай одним переводом и точно в указанной сумме.\n"
        f"Если платёж не проходит или есть тех. вопрос — поддержка: @PaySyncSupportBot\n\n"
        f"После перевода нажми кнопку проверки ниже."
    )


def render_crypto_message(inv: asyncpg.Record) -> str:
    trade_id = str(inv["trade_id"])
    pay_url = str(inv["pay_url"] or "").strip()
    amount_to_pay = str(inv["amount_int"])
    currency = str(inv["currency"])
    expires_at = inv["expires_at"]

    return (
        f"🪙 Оплата через Crypto\n\n"
        f"🧾 Заявка: {trade_id}\n"
        f"💰 Сумма: {amount_to_pay} {currency}\n"
        f"🔗 Ссылка на оплату:\n{pay_url}\n"
        f"⏳ Срок оплаты: до {safe_dt_to_text(expires_at)}\n\n"
        f"После оплаты нажми кнопку проверки ниже."
    )


# ================== FSM ==================
class PromoStates(StatesGroup):
    waiting_code = State()


class TopupStates(StatesGroup):
    waiting_amount = State()


# ================== BOT ==================
dp = Dispatcher(storage=MemoryStorage())


# ================== HANDLERS ==================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    text = await render_main_text(message.from_user.id)
    await message.answer(text, reply_markup=bottom_menu())


@dp.message(F.text.contains("ГЛАВНАЯ"))
async def btn_main(message: Message):
    text = await render_main_text(message.from_user.id)
    await message.answer(text, reply_markup=inline_main_city())


@dp.message(F.text.contains("ПРОФИЛЬ"))
async def btn_profile(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_user_stats(message.from_user.id)
    text = PROFILE_TEXT_TEMPLATE.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_profile_menu())


@dp.message(F.text.contains("ПОМОЩЬ"))
async def btn_help(message: Message):
    await message.answer(HELP_TEXT, reply_markup=bottom_menu())


@dp.message(F.text.contains("РАБОТА"))
async def btn_work(message: Message):
    await message.answer(WORK_TEXT, reply_markup=bottom_menu())


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data == "city:odesa")
async def cb_city_odesa(call: CallbackQuery):
    await call.answer()
    rows = await get_city_products("odesa")
    await call.message.answer(
        "✅ Вы выбрали город Одесса.\nВыберите товар:",
        reply_markup=inline_city_products(rows, "odesa")
    )


@dp.callback_query(F.data.startswith("prod:"))
async def cb_product(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) != 3:
        return
    code = parts[2]

    product = await get_product(code)
    if not product or not product["is_active"] or product["sold_at"] is not None:
        await call.message.answer("❌ Товар недоступен.")
        return

    reserved_by = product["reserved_by"]
    reserved_until = product["reserved_until"]
    if reserved_until and reserved_until < utc_now():
        reserved_by = None
        reserved_until = None

    if reserved_by and reserved_until and reserved_until > utc_now() and int(reserved_by) != call.from_user.id:
        await call.message.answer("❌ Этот товар сейчас временно забронирован другим покупателем.")
        return

    name = str(product["name"])
    price = decimal.Decimal(product["price"])
    desc = str(product["description"] or "").strip() or " "

    text = ITEM_TEXT_TEMPLATE.format(name=name, price=f"{price:.2f}", uah=UAH, desc=desc)
    await call.message.answer(text, reply_markup=inline_one_button("Район", f"district:{code}"))


@dp.callback_query(F.data.startswith("district:"))
async def cb_district(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":", 1)[1]
    await call.message.answer(DISTRICT_TEXT, reply_markup=inline_pay_buttons(code))


@dp.callback_query(F.data.startswith("pay:bal:"))
async def cb_pay_balance(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[-1]

    ok, msg = await reserve_product(call.from_user.id, code)
    if not ok:
        await call.message.answer(msg)
        return

    try:
        ok2, msg2 = await buy_with_balance(call.from_user.id, code)
    except Exception as e:
        await release_product_reservation(code)
        await call.message.answer(f"❌ Ошибка оплаты балансом: {e}")
        return

    await call.message.answer(msg2)


@dp.callback_query(F.data.startswith("pay:card:"))
async def cb_pay_card(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[-1]

    ok, msg = await reserve_product(call.from_user.id, code)
    if not ok:
        await call.message.answer(msg)
        return

    product = await get_product(code)
    if not product:
        await release_product_reservation(code)
        await call.message.answer("❌ Товар не найден.")
        return

    price = decimal.Decimal(product["price"])
    logical_amount_int = price_to_int_uah(price)
    if logical_amount_int is None:
        await release_product_reservation(code)
        await call.message.answer("❌ Для оплаты картой цена товара должна быть целым числом (например 350.00).")
        return

    try:
        inv = await invoice_create_paysync(call.from_user.id, "product", logical_amount_int, code)
    except Exception as e:
        await release_product_reservation(code)
        await call.message.answer(f"❌ Ошибка создания оплаты: {e}")
        return

    await call.message.answer(
        render_h2h_message(inv),
        reply_markup=inline_check_and_copy(str(inv["trade_id"]), str(inv["card_number"] or "").strip() or None),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data.startswith("pay:crypto:"))
async def cb_pay_crypto(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[-1]

    ok, msg = await reserve_product(call.from_user.id, code)
    if not ok:
        await call.message.answer(msg)
        return

    product = await get_product(code)
    if not product:
        await release_product_reservation(code)
        await call.message.answer("❌ Товар не найден.")
        return

    price = decimal.Decimal(product["price"])
    logical_amount_int = price_to_int_uah(price)
    if logical_amount_int is None:
        await release_product_reservation(code)
        await call.message.answer("❌ Для оплаты crypto цена товара должна быть целым числом (например 350.00).")
        return

    try:
        inv = await invoice_create_crypto(
            call.from_user.id,
            logical_amount_int,
            "product",
            code,
            f"{str(product['name'])} | {logical_amount_int} {CRYPTO_PAY_FIAT}",
        )
    except Exception as e:
        await release_product_reservation(code)
        await call.message.answer(f"❌ Ошибка создания Crypto оплаты: {e}")
        return

    await call.message.answer(render_crypto_message(inv), reply_markup=inline_check_only(str(inv["trade_id"])))


@dp.callback_query(F.data == "profile:topup")
async def cb_profile_topup(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await call.message.answer("Выбери способ пополнения:", reply_markup=inline_topup_methods())


@dp.callback_query(F.data.startswith("topup_method:"))
async def cb_topup_method(call: CallbackQuery, state: FSMContext):
    await call.answer()
    provider = call.data.split(":", 1)[1]
    await state.set_state(TopupStates.waiting_amount)
    await state.update_data(topup_provider=provider)
    if provider == "crypto":
        await call.message.answer(f"🪙 Пополнение через Crypto\n\n{TOPUP_ASK_TEXT}")
    else:
        await call.message.answer(f"💳 Пополнение через PaySync\n\n{TOPUP_ASK_TEXT}")


@dp.message(TopupStates.waiting_amount)
async def topup_amount_entered(message: Message, state: FSMContext):
    logical_amount_int = parse_int_amount(message.text)
    if logical_amount_int is None:
        await message.answer("❌ Введи сумму целым числом. Пример: 200")
        return

    if logical_amount_int < 10:
        await message.answer(f"❌ Минимум 10 {UAH}.")
        return

    data = await state.get_data()
    provider = str(data.get("topup_provider") or "paysync")

    try:
        if provider == "crypto":
            inv = await invoice_create_crypto(
                message.from_user.id,
                logical_amount_int,
                "topup",
                None,
                f"Пополнение баланса | {logical_amount_int} {CRYPTO_PAY_FIAT}",
            )
            await message.answer(render_crypto_message(inv), reply_markup=inline_check_only(str(inv["trade_id"])))
        else:
            inv = await invoice_create_paysync(message.from_user.id, "topup", logical_amount_int, None)
            await message.answer(
                render_h2h_message(inv),
                reply_markup=inline_check_and_copy(str(inv["trade_id"]), str(inv["card_number"] or "").strip() or None),
                parse_mode="Markdown"
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка создания оплаты: {e}")
        await state.clear()
        return

    await state.clear()


@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]
    try:
        ok, msg = await invoice_apply_paid(trade_id)
    except Exception as e:
        await call.message.answer(f"❌ Ошибка проверки оплаты: {e}")
        return
    await call.message.answer(msg)


@dp.callback_query(F.data == "profile:promo")
async def cb_profile_promo(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PromoStates.waiting_code)
    await call.message.answer("🎟 Введи промокод одним сообщением:")


@dp.message(PromoStates.waiting_code)
async def promo_entered(message: Message, state: FSMContext):
    await ensure_user(message.from_user.id)
    ok, msg = await activate_promo(message.from_user.id, message.text)
    await message.answer(msg)
    await state.clear()


@dp.callback_query(F.data == "profile:history")
async def cb_profile_history(call: CallbackQuery):
    await call.answer()
    rows = await get_history(call.from_user.id)
    if not rows:
        await call.message.answer("История пуста.")
        return

    text = "🧾 История покупок:\n\n"
    for r in rows:
        dt = r["created_at"].strftime("%Y-%m-%d %H:%M")
        price = decimal.Decimal(r["price"])
        provider = str(r["provider"] or "unknown")
        text += f"• {r['item_name']} — {price:.2f} {UAH} [{provider}] ({dt})\n{r['link']}\n\n"
    await call.message.answer(text)


# ================== ADMIN COMMANDS ==================
@dp.message(F.text.startswith("/addproduct"))
async def cmd_addproduct(message: Message):
    if not is_admin(message.from_user.id):
        return

    raw = message.text.strip()
    try:
        parts = [p.strip() for p in raw[len("/addproduct"):].strip().split("|")]
        if len(parts) < 5:
            await message.answer("Формат:\n/addproduct city | code | name | price | link | desc(опц.)")
            return

        city = parts[0].lower()
        code = parts[1].strip()
        name = parts[2].strip()
        price = decimal.Decimal(parts[3].replace(",", "."))
        link = parts[4].strip()
        desc = parts[5].strip() if len(parts) >= 6 else ""

        if not code:
            await message.answer("❌ code пустой.")
            return
        if not name:
            await message.answer("❌ name пустой.")
            return
        if not link:
            await message.answer("❌ link пустой.")
            return

        await add_or_update_product(city, code, name, price, link, desc)
        await message.answer(f"✅ Товар сохранён: {code} ({name}) — {price:.2f} {UAH}")

    except Exception as e:
        await message.answer(f"❌ Ошибка формата: {e}")


@dp.message(F.text.startswith("/delproduct"))
async def cmd_delproduct(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /delproduct CODE")
        return
    code = parts[1].strip()
    ok = await deactivate_product(code)
    await message.answer("✅ Отключено." if ok else "❌ Не найдено.")


@dp.message(F.text.startswith("/products"))
async def cmd_products(message: Message):
    if not is_admin(message.from_user.id):
        return
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT city, code, name, price, is_active, reserved_until, sold_at
            FROM products
            ORDER BY created_at DESC
            LIMIT 50
            """
        )
    if not rows:
        await message.answer("Товаров нет.")
        return
    text = "Товары:\n\n"
    for r in rows:
        state = "ON" if r["is_active"] else "OFF"
        if r["sold_at"] is not None:
            state = "SOLD"
        elif r["reserved_until"] is not None and r["reserved_until"] > utc_now():
            state = f"RESERVED до {safe_dt_to_text(r['reserved_until'])}"
        text += f"{r['city']} | {r['code']} | {r['name']} | {decimal.Decimal(r['price']):.2f} {UAH} | {state}\n"
    await message.answer(text)


@dp.message(F.text.startswith("/freeproduct"))
async def cmd_freeproduct(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /freeproduct CODE")
        return
    code = parts[1].strip()
    await release_product_reservation(code)
    await cancel_waiting_invoices_for_product(code)
    await message.answer("✅ Бронь по товару снята, ожидающие счета отменены.")


@dp.message(F.text.startswith("/cancelinvoice"))
async def cmd_cancelinvoice(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /cancelinvoice TRADE_ID")
        return
    trade_id = parts[1].strip()
    ok, msg = await cancel_waiting_invoice(trade_id)
    await message.answer(msg)


@dp.message(F.text.startswith("/invoice"))
async def cmd_invoice(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /invoice TRADE_ID")
        return
    trade_id = parts[1].strip()

    assert pool is not None
    async with pool.acquire() as con:
        inv = await con.fetchrow(
            """
            SELECT trade_id, provider, kind, status, amount_int, amount, currency,
                   product_code, card_number, external_id, pay_url, expires_at, paid_at
            FROM invoices
            WHERE trade_id=$1
            """,
            trade_id
        )
    if not inv:
        await message.answer("❌ Счёт не найден.")
        return

    text = (
        f"trade_id: {inv['trade_id']}\n"
        f"provider: {inv['provider']}\n"
        f"kind: {inv['kind']}\n"
        f"status: {inv['status']}\n"
        f"amount_int: {inv['amount_int']}\n"
        f"amount: {inv['amount']}\n"
        f"currency: {inv['currency']}\n"
        f"product_code: {inv['product_code']}\n"
        f"card_number: {inv['card_number']}\n"
        f"external_id: {inv['external_id']}\n"
        f"pay_url: {inv['pay_url']}\n"
        f"expires_at: {safe_dt_to_text(inv['expires_at'])}\n"
        f"paid_at: {safe_dt_to_text(inv['paid_at'])}\n"
    )
    await message.answer(text)


async def main():
    await db_init()
    bot = Bot(token=BOT_TOKEN)

    cleanup_task = asyncio.create_task(background_cleanup_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
