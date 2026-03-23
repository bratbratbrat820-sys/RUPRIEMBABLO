import os
import asyncio
import asyncpg
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

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

# =========================
# ENV
# =========================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
ADMIN_ID_RAW = (os.getenv("ADMIN_ID") or "").strip()

PAYSYNC_APIKEY = (os.getenv("PAYSYNC_APIKEY") or "").strip()
PAYSYNC_CLIENT_ID = (os.getenv("PAYSYNC_CLIENT_ID") or "").strip()

PAYSYNC_DEFAULT_CURRENCY = (os.getenv("PAYSYNC_DEFAULT_CURRENCY") or "RUB").strip().upper()
PAYMENT_TIMEOUT_MINUTES = int((os.getenv("PAYMENT_TIMEOUT_MINUTES") or "15").strip())
RESERVATION_MINUTES = int((os.getenv("RESERVATION_MINUTES") or "15").strip())

SHOP_TITLE = (os.getenv("SHOP_TITLE") or "Digital Access Store").strip()
SUPPORT_USERNAME = (os.getenv("SUPPORT_USERNAME") or "YOUR_SUPPORT_USERNAME").strip().lstrip("@")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")
if not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("ADMIN_ID is missing or invalid")
if not PAYSYNC_APIKEY:
    raise RuntimeError("PAYSYNC_APIKEY is missing")
if not PAYSYNC_CLIENT_ID.isdigit():
    raise RuntimeError("PAYSYNC_CLIENT_ID is missing or invalid")

ADMIN_ID = int(ADMIN_ID_RAW)
PAYSYNC_CLIENT_ID = int(PAYSYNC_CLIENT_ID)

bot = Bot(BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())
db_pool: asyncpg.Pool | None = None

UTC = timezone.utc


# =========================
# TEXTS
# =========================
START_TEXT = f"""<b>{SHOP_TITLE}</b>

Digital access packages with instant processing and clean checkout.

• Secure payment
• Fast confirmation
• Easy catalog
• Direct delivery after successful payment

Support: @{SUPPORT_USERNAME}

Choose an action below:"""

ABOUT_TEXT = """<b>About</b>

This store offers digital access packages and onboarding materials with a fast purchase flow and automatic delivery after payment confirmation."""

SUPPORT_TEXT = f"""<b>Support</b>

For questions about payment or delivery:
@{SUPPORT_USERNAME}"""

ORDERS_EMPTY_TEXT = """<b>My orders</b>

You don't have any completed orders yet."""

PAYMENT_WAIT_TEXT = """<b>Payment created</b>

Invoice: <code>{trade}</code>
Card: <code>{card}</code>
Amount: <b>{amount} {currency}</b>
Valid until: <b>{expires_at}</b>

Transfer the exact amount in one payment, then press the button below to check status.
"""

PAYMENT_ALREADY_PAID = "✅ This invoice has already been confirmed."
PAYMENT_NOT_FOUND = "Invoice not found."
PAYMENT_STILL_WAITING = "Payment has not been confirmed yet. If you already paid, wait a bit and check again."
PAYMENT_EXPIRED = "⏰ Payment time has expired. Create a new order."

DELIVERY_TEXT = """✅ <b>Payment confirmed</b>

Product: <b>{name}</b>

Your delivery:
{delivery}
"""

CATALOG_HEADER = "<b>Catalog</b>\n\nChoose a product:"
PROFILE_TEXT = "<b>Profile</b>\n\nTelegram ID: <code>{user_id}</code>\nCompleted orders: <b>{count}</b>"


# =========================
# DEMO / SAFE PRODUCTS
# =========================
seed_products = [
    {
        "code": "klein_pack",
        "name": "Kleinanzeigen Access Pack",
        "price_int": 5490,
        "currency": "RUB",
        "description": (
            "Structured access package for marketplace onboarding.\n\n"
            "Includes:\n"
            "• onboarding notes\n"
            "• workflow checklist\n"
            "• access instructions\n"
            "• delivery after payment confirmation"
        ),
        "delivery": (
            "Kleinanzeigen Access Pack\n"
            "Format: digital delivery\n"
            "Status: ready\n\n"
            "To receive the final material set, contact support with your paid invoice ID."
        ),
    },
    {
        "code": "tutti_pack",
        "name": "Tutti Access Pack",
        "price_int": 4990,
        "currency": "RUB",
        "description": (
            "Digital access package with onboarding guidance and ready-to-use delivery flow.\n\n"
            "Includes:\n"
            "• starter instructions\n"
            "• structured access notes\n"
            "• quick launch checklist\n"
            "• post-payment delivery"
        ),
        "delivery": (
            "Tutti Access Pack\n"
            "Format: digital delivery\n"
            "Status: ready\n\n"
            "To receive the final material set, contact support with your paid invoice ID."
        ),
    },
]


# =========================
# DB
# =========================
async def create_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


async def init_db():
    assert db_pool is not None
    async with db_pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            price_int INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'RUB',
            delivery TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            is_sold BOOLEAN NOT NULL DEFAULT FALSE,
            reserved_by BIGINT,
            reserved_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            invoice_key TEXT UNIQUE NOT NULL,
            user_id BIGINT NOT NULL,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            amount_int INTEGER NOT NULL,
            currency TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            paysync_trade_id TEXT,
            paysync_card_number TEXT,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            paid_at TIMESTAMPTZ
        );
        """)

        await con.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            product_id INTEGER NOT NULL,
            invoice_id INTEGER NOT NULL,
            delivered_text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        for item in seed_products:
            await con.execute("""
            INSERT INTO products (code, name, description, price_int, currency, delivery, is_active, is_sold)
            VALUES ($1, $2, $3, $4, $5, $6, TRUE, FALSE)
            ON CONFLICT (code) DO UPDATE
            SET name = EXCLUDED.name,
                description = EXCLUDED.description,
                price_int = EXCLUDED.price_int,
                currency = EXCLUDED.currency,
                delivery = EXCLUDED.delivery,
                is_active = TRUE
            """,
            item["code"],
            item["name"],
            item["description"],
            item["price_int"],
            item["currency"],
            item["delivery"])


async def upsert_user(message: Message):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        await con.execute("""
        INSERT INTO users (user_id, username, first_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO UPDATE
        SET username = EXCLUDED.username,
            first_name = EXCLUDED.first_name
        """,
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name or "")


# =========================
# PAYSYNC
# =========================
def format_amount(amount_int: int) -> str:
    rub = Decimal(amount_int) / Decimal("100")
    return str(rub.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


async def paysync_create_invoice(amount_int: int, currency: str, user_id: int, product_code: str):
    amount = format_amount(amount_int)
    payload = {
        "client": PAYSYNC_CLIENT_ID,
        "amount": amount,
        "currency": currency,
        "data": f"user={user_id};product={product_code};nonce={uuid.uuid4().hex[:12]}"
    }
    headers = {
        "apikey": PAYSYNC_APIKEY,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.paysync.bot/merchant/create",
            json=payload,
            headers=headers,
            timeout=30
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"PaySync create error: {data}")

    trade = str(data.get("trade") or "")
    card = str(data.get("number") or data.get("card") or "")
    status = str(data.get("status") or "wait")
    if not trade:
        raise RuntimeError(f"PaySync returned no trade: {data}")
    return {
        "trade": trade,
        "card": card,
        "status": status,
        "raw": data,
    }


async def paysync_check_invoice(trade_id: str):
    headers = {
        "apikey": PAYSYNC_APIKEY,
        "Content-Type": "application/json",
    }
    payload = {
        "client": PAYSYNC_CLIENT_ID,
        "trade": trade_id,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.paysync.bot/merchant/gettrans",
            json=payload,
            headers=headers,
            timeout=30
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"PaySync check error: {data}")

    return {
        "status": str(data.get("status") or "").lower(),
        "raw": data,
    }


# =========================
# HELPERS
# =========================
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Catalog"), KeyboardButton(text="📦 My orders")],
            [KeyboardButton(text="👤 Profile"), KeyboardButton(text="💬 Support")],
            [KeyboardButton(text="ℹ️ About")],
        ],
        resize_keyboard=True,
    )


def start_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open catalog", callback_data="catalog")],
        [InlineKeyboardButton(text="📦 My orders", callback_data="my_orders")],
        [InlineKeyboardButton(text="💬 Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
    ])


def catalog_inline(products: list[asyncpg.Record]) -> InlineKeyboardMarkup:
    rows = []
    for p in products:
        rows.append([
            InlineKeyboardButton(
                text=f"{p['name']} — {p['price_int'] / 100:.2f} {p['currency']}",
                callback_data=f"product:{p['code']}"
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def product_inline(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Buy now", callback_data=f"buy:{code}")],
        [InlineKeyboardButton(text="⬅ Back to catalog", callback_data="catalog")],
    ])


def payment_inline(invoice_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Check payment", callback_data=f"check:{invoice_key}")],
        [InlineKeyboardButton(text="💬 Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
        [InlineKeyboardButton(text="⬅ Back to catalog", callback_data="catalog")],
    ])


async def get_active_products():
    assert db_pool is not None
    async with db_pool.acquire() as con:
        return await con.fetch("""
        SELECT *
        FROM products
        WHERE is_active = TRUE
          AND is_sold = FALSE
          AND (reserved_until IS NULL OR reserved_until < NOW() OR reserved_by IS NULL)
        ORDER BY id ASC
        """)


async def get_product_by_code(code: str):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        return await con.fetchrow("""
        SELECT *
        FROM products
        WHERE code = $1
        LIMIT 1
        """, code)


async def reserve_product(product_id: int, user_id: int) -> bool:
    assert db_pool is not None
    async with db_pool.acquire() as con:
        result = await con.execute("""
        UPDATE products
        SET reserved_by = $2,
            reserved_until = NOW() + ($3::text || ' minutes')::interval
        WHERE id = $1
          AND is_active = TRUE
          AND is_sold = FALSE
          AND (
                reserved_until IS NULL
                OR reserved_until < NOW()
                OR reserved_by IS NULL
                OR reserved_by = $2
          )
        """, product_id, user_id, RESERVATION_MINUTES)
        return result.endswith("1")


async def create_invoice(user_id: int, product: asyncpg.Record, paysync_data: dict):
    assert db_pool is not None
    invoice_key = uuid.uuid4().hex
    expires_at = datetime.now(UTC) + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    async with db_pool.acquire() as con:
        row = await con.fetchrow("""
        INSERT INTO invoices (
            invoice_key, user_id, product_id, amount_int, currency, status,
            paysync_trade_id, paysync_card_number, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $8)
        RETURNING *
        """,
        invoice_key,
        user_id,
        product["id"],
        product["price_int"],
        product["currency"],
        paysync_data["trade"],
        paysync_data["card"],
        expires_at)
    return row


async def mark_invoice_paid(invoice_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        await con.execute("""
        UPDATE invoices
        SET status = 'paid', paid_at = NOW()
        WHERE id = $1
        """, invoice_id)


async def deliver_invoice(invoice: asyncpg.Record):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        product = await con.fetchrow("SELECT * FROM products WHERE id = $1", invoice["product_id"])
        if not product:
            return None

        existing = await con.fetchrow("""
        SELECT * FROM purchases WHERE invoice_id = $1 LIMIT 1
        """, invoice["id"])
        if existing:
            return {
                "already": True,
                "product": product,
                "delivery": existing["delivered_text"],
            }

        await con.execute("""
        UPDATE products
        SET is_sold = TRUE,
            reserved_by = NULL,
            reserved_until = NULL
        WHERE id = $1
        """, product["id"])

        await con.execute("""
        INSERT INTO purchases (user_id, product_id, invoice_id, delivered_text)
        VALUES ($1, $2, $3, $4)
        """,
        invoice["user_id"],
        product["id"],
        invoice["id"],
        product["delivery"])

        return {
            "already": False,
            "product": product,
            "delivery": product["delivery"],
        }


async def get_invoice_by_key(invoice_key: str):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        return await con.fetchrow("""
        SELECT * FROM invoices WHERE invoice_key = $1 LIMIT 1
        """, invoice_key)


async def get_user_purchase_count(user_id: int) -> int:
    assert db_pool is not None
    async with db_pool.acquire() as con:
        value = await con.fetchval("""
        SELECT COUNT(*) FROM purchases WHERE user_id = $1
        """, user_id)
        return int(value or 0)


async def get_user_orders(user_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as con:
        return await con.fetch("""
        SELECT p.name, p.currency, i.amount_int, i.paid_at
        FROM purchases pu
        JOIN products p ON p.id = pu.product_id
        JOIN invoices i ON i.id = pu.invoice_id
        WHERE pu.user_id = $1
        ORDER BY pu.id DESC
        """, user_id)


async def cleanup_task():
    while True:
        try:
            assert db_pool is not None
            async with db_pool.acquire() as con:
                await con.execute("""
                UPDATE products
                SET reserved_by = NULL, reserved_until = NULL
                WHERE reserved_until IS NOT NULL
                  AND reserved_until < NOW()
                  AND is_sold = FALSE
                """)

                await con.execute("""
                UPDATE invoices
                SET status = 'expired'
                WHERE status = 'pending'
                  AND expires_at < NOW()
                """)
        except Exception as e:
            print("cleanup_task error:", e)
        await asyncio.sleep(30)


# =========================
# RENDER
# =========================
async def render_catalog(target):
    products = await get_active_products()
    text = CATALOG_HEADER
    markup = catalog_inline(products)
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=markup)
    else:
        await target.answer(text, reply_markup=markup)


async def render_orders(message: Message):
    orders = await get_user_orders(message.from_user.id)
    if not orders:
        await message.answer(ORDERS_EMPTY_TEXT, reply_markup=main_menu())
        return

    lines = ["<b>My orders</b>\n"]
    for item in orders:
        paid_at = item["paid_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC") if item["paid_at"] else "-"
        lines.append(
            f"• <b>{item['name']}</b>\n"
            f"  Amount: <b>{item['amount_int'] / 100:.2f} {item['currency']}</b>\n"
            f"  Paid: {paid_at}\n"
        )
    await message.answer("\n".join(lines), reply_markup=main_menu())


# =========================
# HANDLERS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    await upsert_user(message)
    await message.answer(START_TEXT, reply_markup=main_menu())
    await message.answer("Quick actions:", reply_markup=start_inline())


@dp.message(F.text == "🛍 Catalog")
async def msg_catalog(message: Message):
    await render_catalog(message)


@dp.message(F.text == "📦 My orders")
async def msg_orders(message: Message):
    await render_orders(message)


@dp.message(F.text == "👤 Profile")
async def msg_profile(message: Message):
    count = await get_user_purchase_count(message.from_user.id)
    await message.answer(
        PROFILE_TEXT.format(user_id=message.from_user.id, count=count),
        reply_markup=main_menu()
    )


@dp.message(F.text == "💬 Support")
async def msg_support(message: Message):
    await message.answer(SUPPORT_TEXT, reply_markup=main_menu())


@dp.message(F.text == "ℹ️ About")
async def msg_about(message: Message):
    await message.answer(ABOUT_TEXT, reply_markup=main_menu())


@dp.callback_query(F.data == "home")
async def cb_home(call: CallbackQuery):
    await call.message.edit_text(START_TEXT, reply_markup=start_inline())
    await call.answer()


@dp.callback_query(F.data == "catalog")
async def cb_catalog(call: CallbackQuery):
    await render_catalog(call)
    await call.answer()


@dp.callback_query(F.data == "my_orders")
async def cb_my_orders(call: CallbackQuery):
    orders = await get_user_orders(call.from_user.id)
    if not orders:
        await call.message.edit_text(ORDERS_EMPTY_TEXT)
    else:
        lines = ["<b>My orders</b>\n"]
        for item in orders:
            paid_at = item["paid_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC") if item["paid_at"] else "-"
            lines.append(
                f"• <b>{item['name']}</b>\n"
                f"  Amount: <b>{item['amount_int'] / 100:.2f} {item['currency']}</b>\n"
                f"  Paid: {paid_at}\n"
            )
        await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="home")]]
        ))
    await call.answer()


@dp.callback_query(F.data.startswith("product:"))
async def cb_product(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    product = await get_product_by_code(code)
    if not product or not product["is_active"] or product["is_sold"]:
        await call.answer("This product is unavailable.", show_alert=True)
        return

    text = (
        f"<b>{product['name']}</b>\n\n"
        f"Price: <b>{product['price_int'] / 100:.2f} {product['currency']}</b>\n\n"
        f"{product['description']}"
    )
    await call.message.edit_text(text, reply_markup=product_inline(code))
    await call.answer()


@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    product = await get_product_by_code(code)
    if not product or not product["is_active"] or product["is_sold"]:
        await call.answer("This product is unavailable.", show_alert=True)
        return

    reserved = await reserve_product(product["id"], call.from_user.id)
    if not reserved:
        await call.answer("This product is temporarily reserved by another customer.", show_alert=True)
        return

    try:
        paysync_data = await paysync_create_invoice(
            product["price_int"],
            product["currency"],
            call.from_user.id,
            product["code"]
        )
    except Exception as e:
        await call.answer("Failed to create payment. Try again later.", show_alert=True)
        print("paysync_create_invoice error:", e)
        return

    invoice = await create_invoice(call.from_user.id, product, paysync_data)
    expires_at = invoice["expires_at"].astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")

    text = PAYMENT_WAIT_TEXT.format(
        trade=invoice["paysync_trade_id"],
        card=invoice["paysync_card_number"] or "Card not provided",
        amount=f"{invoice['amount_int'] / 100:.2f}",
        currency=invoice["currency"],
        expires_at=expires_at
    )
    await call.message.edit_text(text, reply_markup=payment_inline(invoice["invoice_key"]))
    await call.answer()


@dp.callback_query(F.data.startswith("check:"))
async def cb_check_payment(call: CallbackQuery):
    invoice_key = call.data.split(":", 1)[1]
    invoice = await get_invoice_by_key(invoice_key)
    if not invoice:
        await call.answer(PAYMENT_NOT_FOUND, show_alert=True)
        return

    if invoice["user_id"] != call.from_user.id and call.from_user.id != ADMIN_ID:
        await call.answer("Access denied.", show_alert=True)
        return

    if invoice["status"] == "paid":
        await call.answer(PAYMENT_ALREADY_PAID, show_alert=True)
        return

    if invoice["status"] == "expired" or invoice["expires_at"] < datetime.now(UTC):
        await call.answer(PAYMENT_EXPIRED, show_alert=True)
        return

    try:
        status_data = await paysync_check_invoice(invoice["paysync_trade_id"])
    except Exception as e:
        await call.answer("Failed to check payment. Try again in a moment.", show_alert=True)
        print("paysync_check_invoice error:", e)
        return

    if status_data["status"] != "paid":
        await call.answer(PAYMENT_STILL_WAITING, show_alert=True)
        return

    await mark_invoice_paid(invoice["id"])
    delivery_data = await deliver_invoice(invoice)
    if not delivery_data:
        await call.answer("Delivery error.", show_alert=True)
        return

    text = DELIVERY_TEXT.format(
        name=delivery_data["product"]["name"],
        delivery=delivery_data["delivery"]
    )
    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Back to catalog", callback_data="catalog")],
            [InlineKeyboardButton(text="💬 Support", url=f"https://t.me/{SUPPORT_USERNAME}")]
        ])
    )
    await call.answer("Payment confirmed.", show_alert=True)


@dp.message(F.text.startswith("/addproduct"))
async def admin_add_product(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split("|")
    if len(parts) != 6:
        await message.answer(
            "Format:\n"
            "/addproduct code | name | price_rub | currency | description | delivery"
        )
        return

    left = parts[0].strip()
    if not left.startswith("/addproduct"):
        await message.answer("Invalid command.")
        return

    code = left.replace("/addproduct", "").strip()
    name = parts[1].strip()
    price_rub = parts[2].strip()
    currency = parts[3].strip().upper()
    description = parts[4].strip()
    delivery = parts[5].strip()

    if not code:
        await message.answer("Product code is required.")
        return

    try:
        price_int = int((Decimal(price_rub) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except Exception:
        await message.answer("Invalid price.")
        return

    assert db_pool is not None
    async with db_pool.acquire() as con:
        await con.execute("""
        INSERT INTO products (code, name, description, price_int, currency, delivery, is_active, is_sold)
        VALUES ($1, $2, $3, $4, $5, $6, TRUE, FALSE)
        ON CONFLICT (code) DO UPDATE
        SET name = EXCLUDED.name,
            description = EXCLUDED.description,
            price_int = EXCLUDED.price_int,
            currency = EXCLUDED.currency,
            delivery = EXCLUDED.delivery,
            is_active = TRUE
        """, code, name, description, price_int, currency, delivery)

    await message.answer(f"✅ Product saved: <b>{name}</b>")


@dp.message()
async def fallback(message: Message):
    await message.answer("Use the menu buttons below.", reply_markup=main_menu())


# =========================
# MAIN
# =========================
async def main():
    await create_pool()
    await init_db()
    asyncio.create_task(cleanup_task())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
