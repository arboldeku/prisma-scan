"""
Prisma Scan — Punto de Venta Físico
Sistema standalone de registro de ventas para Prisma, tienda de cartas Pokémon.
Genera CSV diarios para su posterior carga manual a Drive → Pipeline Bronze.
"""

import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ_MADRID = ZoneInfo("Europe/Madrid")

import gspread
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from google.oauth2.service_account import Credentials

# Componente custom de escaneo (detecta velocidad de escáner, mantiene foco)
_SCANNER_DIR = Path(__file__).parent / "_scanner_component"
_scanner_input = components.declare_component("scanner_input", path=str(_SCANNER_DIR))

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
CATALOG_PATH  = Path("data/hits_catalog.csv")
STORE_CATALOG = Path("data/store_hits_catalog.csv")
SALES_DIR    = Path("sales_output")
TODAY        = datetime.now(TZ_MADRID).strftime("%Y-%m-%d")
DAILY_CSV    = SALES_DIR / f"sales_physical_scan_{TODAY}.csv"

USE_SHEETS    = "gcp_service_account" in st.secrets
USE_SUPABASE  = "supabase" in st.secrets

CSV_COLUMNS = [
    "sale_event_id",
    "sale_ts",
    "session_id",
    "internal_sku",
    "display_name",
    "language",
    "business_rarity",
    "qty",
    "unit_price",
    "gross_amount",
    "discount_eur",
    "channel",
    "source_system",
    "status",
    "sale_type",
    "payment_method",
    "money_direction",
    "trade_amount",
]

# ─────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Prisma · Scan",
    page_icon="⚡",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
# CSS — Tema oscuro para tablet
# ─────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=DM+Sans:wght@400;500;700&display=swap');

:root {
    --prisma-bg:      #0a0a0f;
    --prisma-surface: #141420;
    --prisma-border:  #2a2a3d;
    --prisma-text:    #e8e8f0;
    --prisma-muted:   #8888aa;
    --prisma-accent:  #f0c040;
    --prisma-success: #40d080;
    --prisma-danger:  #f04060;
    --prisma-info:    #40a0f0;
}

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background-color: var(--prisma-bg) !important;
    color: var(--prisma-text) !important;
    font-family: 'DM Sans', sans-serif !important;
}

[data-testid="stAppViewBlockContainer"] {
    max-width: 700px !important;
    padding: 0.8rem 1.2rem !important;
}

#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebar"] { display: none !important; }
.block-container { padding-top: 0.5rem !important; }

/* Header */
.prisma-header {
    text-align: center;
    padding: 1rem 0 0.8rem 0;
    border-bottom: 1px solid var(--prisma-border);
    margin-bottom: 1rem;
}
.prisma-logo {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    color: var(--prisma-accent);
    letter-spacing: 0.08em;
    margin: 0;
}
.prisma-subtitle {
    font-size: 0.75rem;
    color: var(--prisma-muted);
    margin: 0.2rem 0 0 0;
}

/* Alertas */
.alert {
    padding: 0.7rem 1rem;
    border-radius: 8px;
    font-size: 0.88rem;
    font-weight: 500;
    margin: 0.6rem 0;
}
.alert-ok    { background: #002a10; border: 1px solid #005a20; color: var(--prisma-success); }
.alert-warn  { background: #2a2000; border: 1px solid #5a4a00; color: var(--prisma-accent); }
.alert-error { background: #2a0010; border: 1px solid #5a0020; color: var(--prisma-danger); }

/* Pills de tipo venta/cambio */
.pill-venta  { background:#003d1a; color:#4cdf80; border:1px solid #005a20;
               border-radius:10px; padding:2px 8px; font-size:0.68rem; font-weight:700; }
.pill-cambio { background:#001f4d; color:#5badff; border:1px solid #003080;
               border-radius:10px; padding:2px 8px; font-size:0.68rem; font-weight:700; }

/* Banner post-escaneo venta/cambio */
.type-banner {
    background: #0a1a0a; border: 1px solid #005a20; border-radius: 12px;
    padding: 12px 16px; margin: 6px 0 4px 0;
}
.type-banner-title { color: #4cdf80; font-size: 0.95rem; font-weight: 700; margin-bottom: 6px; }
.timer-bar  { height: 3px; background: #1e2e1e; border-radius: 2px; overflow: hidden; margin-top: 6px; }
.timer-fill { height: 100%; background: #4cdf80;
              animation: shrink3s 3s linear forwards; }
@keyframes shrink3s { from { width: 100%; } to { width: 0%; } }

/* Métricas */
.metric-card {
    background: var(--prisma-surface);
    border: 1px solid var(--prisma-border);
    border-radius: 12px;
    padding: 1rem 0.8rem;
    text-align: center;
}
.metric-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 2rem;
    font-weight: 700;
    color: var(--prisma-accent);
    line-height: 1;
    margin: 0;
}
.metric-label {
    font-size: 0.7rem;
    color: var(--prisma-muted);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 0.3rem;
}

/* Título de sección */
.summary-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    font-weight: 700;
    color: var(--prisma-muted);
    letter-spacing: 0.15em;
    text-transform: uppercase;
    margin: 1.2rem 0 0.5rem 0;
}

/* Footer */
.prisma-footer {
    text-align: center;
    color: var(--prisma-muted);
    font-size: 0.7rem;
    margin-top: 2rem;
    padding-top: 0.8rem;
    border-top: 1px solid var(--prisma-border);
}

/* Input de escaneo — grande y prominente */
.stTextInput > div > div > input {
    background: var(--prisma-surface) !important;
    border: 2px solid var(--prisma-accent) !important;
    color: var(--prisma-text) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.2rem !important;
    border-radius: 10px !important;
    padding: 0.9rem 1rem !important;
}
.stTextInput > div > div > input:focus {
    box-shadow: 0 0 0 3px rgba(240,192,64,0.25) !important;
}

/* Botones */
.stButton > button {
    width: 100% !important;
    border-radius: 10px !important;
    font-weight: 700 !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* Botón ✕ — pequeño y discreto */
[data-testid="stHorizontalBlock"] .stButton > button[kind="secondary"] {
    background: transparent !important;
    border: 1px solid var(--prisma-danger) !important;
    color: var(--prisma-danger) !important;
    font-size: 0.75rem !important;
    padding: 0.2rem 0.4rem !important;
    min-height: 0 !important;
}
[data-testid="stHorizontalBlock"] .stButton > button[kind="secondary"]:hover {
    background: rgba(240,64,96,0.15) !important;
}

/* iframe del componente de escaneo — sin bordes ni scroll */
iframe[title="scanner_input"] {
    border: none !important;
    display: block !important;
}

/* Tabla: eliminar gaps entre filas de st.columns */
[data-testid="stHorizontalBlock"] {
    gap: 0.3rem !important;
    align-items: center !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
# SUPABASE — conexión
# ─────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    """Devuelve cliente Supabase o None si no está configurado."""
    if not USE_SUPABASE:
        return None
    from supabase import create_client
    return create_client(
        st.secrets["supabase"]["url"],
        st.secrets["supabase"]["anon_key"],
    )


# ─────────────────────────────────────────────
# GOOGLE SHEETS — conexión
# ─────────────────────────────────────────────
@st.cache_resource
def get_sheet():
    """Devuelve la hoja de Google Sheets (reconexión automática si cae)."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(st.secrets["sheets"]["spreadsheet_id"]).sheet1
    if sheet.row_count == 0 or not sheet.row_values(1):
        sheet.append_row(CSV_COLUMNS)
    return sheet


# ─────────────────────────────────────────────
# FUNCIONES DE DATOS
# ─────────────────────────────────────────────
@st.cache_data
def load_store_inventory() -> pd.DataFrame:
    """Carga el inventario real de la tienda (store_hits_catalog.csv)."""
    if not STORE_CATALOG.exists():
        return pd.DataFrame()
    df = pd.read_csv(STORE_CATALOG, dtype=str)
    return df


@st.cache_data(ttl=300)
def load_catalog() -> pd.DataFrame:
    """Carga el catálogo operativo.

    Fuente primaria: Supabase inventory_current — tabla única, ya enriquecida con
    todos los campos de catálogo (card_name, lang, is_reverse, rarity, etc.).
    Sin joins adicionales: elimina puntos de fallo y es más rápido.
    Fallback: hits_catalog.csv local.
    """
    sb = get_supabase()
    if sb is not None:
        try:
            # Supabase server default is 1,000 rows/request — paginate to get all rows.
            _COLS = (
                "internal_sku, cardmarket_id, qty, last_updated,"
                "card_name, set_code, set_name, cn, rarity,"
                "lang, is_reverse, condition, listed_price_eur, name_es"
            )
            rows: list = []
            page_size = 1000
            offset = 0
            while True:
                chunk = (
                    sb.table("inventory_current")
                    .select(_COLS)
                    .range(offset, offset + page_size - 1)
                    .execute()
                    .data or []
                )
                rows.extend(chunk)
                if len(chunk) < page_size:
                    break
                offset += page_size

            if rows:
                df = pd.DataFrame(rows, dtype=str)
                df = df.rename(columns={
                    "card_name": "display_name",
                    "lang":      "language",
                    "rarity":    "business_rarity",
                })
                df = df.set_index("internal_sku")
                return df
        except Exception as e:
            st.warning(f"Supabase no disponible, usando catálogo local: {e}")

    # Fallback: CSV local
    if not CATALOG_PATH.exists():
        st.error(f"Catálogo no encontrado en `{CATALOG_PATH}`.")
        st.stop()
    df = pd.read_csv(CATALOG_PATH, dtype={"internal_sku": str, "cardmarket_id": str})
    df = df.rename(columns={
        "card_name": "display_name",
        "lang":      "language",
        "rarity":    "business_rarity",
    })
    df = df.set_index("internal_sku")
    return df


def _suffix_to_lang_rev(suffix: str) -> tuple[list[str], bool | None]:
    """Decodifica sufijo de internal_sku → (candidatos de lang, is_reverse).
    0001 = ESP/JPN reverse-or-only, 0002 = ENG reverse-or-only,
    0003 = ESP/JPN no-reverse,      0004 = ENG no-reverse.
    """
    if suffix == "0001":
        return (["ESP", "JPN"], True)
    if suffix == "0002":
        return (["ENG"], True)
    if suffix == "0003":
        return (["ESP", "JPN"], False)
    if suffix == "0004":
        return (["ENG"], False)
    return ([], None)


@st.cache_data(ttl=3600)
def load_ref_cards() -> pd.DataFrame:
    """Carga ref_cards de Supabase como catálogo de fallback.

    Cubre cualquier carta catalogada en el pipeline aunque no esté en
    inventory_current (e.g. compra recibida pero pendiente de subir).
    Retorna DataFrame vacío si Supabase no está disponible.
    """
    sb = get_supabase()
    if sb is None:
        return pd.DataFrame()
    try:
        rows: list = []
        offset = 0
        page_size = 1000
        while True:
            chunk = (
                sb.table("ref_cards")
                .select("cardmarket_id, card_name, lang, is_reverse, rarity, name_es")
                .range(offset, offset + page_size - 1)
                .execute()
                .data or []
            )
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def load_daily_sales() -> pd.DataFrame:
    """
    Lee las ventas del día actual.
    Sin cache para detectar cambios al instante.
    Fuente primaria: Supabase scan_events. Fallback: Sheets/CSV.
    """
    if USE_SUPABASE:
        try:
            sb = get_supabase()
            if sb is not None:
                resp = (
                    sb.table("scan_events")
                    .select("*")
                    .gte("sale_ts", TODAY)
                    .execute()
                )
                rows = resp.data
                if not rows:
                    return pd.DataFrame(columns=CSV_COLUMNS)
                df = pd.DataFrame(rows)
                for col in CSV_COLUMNS:
                    if col not in df.columns:
                        df[col] = ""
                df["internal_sku"] = df["internal_sku"].astype(str)
                return df[CSV_COLUMNS]
        except Exception:
            pass

    if USE_SHEETS:
        sheet   = get_sheet()
        records = sheet.get_all_records()
        if not records:
            return pd.DataFrame(columns=CSV_COLUMNS)
        df = pd.DataFrame(records)
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[CSV_COLUMNS]
        df["internal_sku"] = df["internal_sku"].astype(str)
        return df[df["sale_ts"].astype(str).str.startswith(TODAY)]

    if not DAILY_CSV.exists():
        return pd.DataFrame(columns=CSV_COLUMNS)
    return pd.read_csv(DAILY_CSV, dtype={"internal_sku": str})


def _write_to_sheets(record: dict) -> None:
    """Escribe en Sheets de forma síncrona. Actualiza cabecera si faltan columnas."""
    try:
        sheet = get_sheet()
        # Sincronizar cabecera: añadir columnas que falten sin borrar las existentes
        headers = sheet.row_values(1)
        for col in CSV_COLUMNS:
            if col not in headers:
                sheet.update_cell(1, len(headers) + 1, col)
                headers.append(col)
        # Escribir valores en el ORDEN DE LA CABECERA del Sheet, no de CSV_COLUMNS.
        # Esto evita que cambios en el orden de CSV_COLUMNS corrompan columnas existentes.
        sheet.append_row(
            [record.get(h, "") for h in headers],
            value_input_option="RAW",
        )
        st.session_state["sheets_error"] = None
    except Exception as e:
        st.session_state["sheets_error"] = str(e)


def _write_to_csv(record: dict) -> None:
    """Escribe en CSV local desde un hilo de fondo."""
    try:
        SALES_DIR.mkdir(exist_ok=True)
        df_new = pd.DataFrame([record])[CSV_COLUMNS]
        df_new.to_csv(DAILY_CSV, mode="a", header=not DAILY_CSV.exists(), index=False)
    except Exception:
        pass


def _write_to_supabase(record: dict) -> None:
    """Escribe una venta en Supabase scan_events."""
    try:
        sb = get_supabase()
        if sb is None:
            return
        row = {k: (None if v == "" else v) for k, v in record.items()}
        sb.table("scan_events").insert(row).execute()
        st.session_state["supabase_error"] = None
    except Exception as e:
        st.session_state["supabase_error"] = str(e)


def save_sale(record: dict) -> None:
    """
    Persiste una venta:
      1. Añade a session_state.sales → UI actualizada al instante.
      2. Escribe en Supabase (primario) o Sheets/CSV (fallback).
    """
    st.session_state.sales.append(record)
    if USE_SUPABASE:
        _write_to_supabase(record)
    elif USE_SHEETS:
        _write_to_sheets(record)
    else:
        _write_to_csv(record)


def register_scan(sku: str) -> tuple[bool, str]:
    """
    Registra el escaneo de un SKU.
    Retorna (éxito, mensaje_feedback).
    """
    ref_product: dict | None = None

    if sku not in catalog.index:
        # Fallback 1: buscar por cardmarket_id en inventory_current
        # (etiquetas impresas sin sufijo, e.g. "869881")
        if "cardmarket_id" in catalog.columns:
            matches = catalog[catalog["cardmarket_id"] == sku]
            if not matches.empty:
                sku = matches.index[0]

        # Fallback 2: buscar en ref_cards descomponiendo el internal_sku
        # Cubre cartas catalogadas pero aún no subidas a inventory_current.
        if sku not in catalog.index and not ref_cards_df.empty:
            parts = sku.rsplit("-", 1)
            if len(parts) == 2:
                cm_id, suffix = parts
                rc = ref_cards_df[ref_cards_df["cardmarket_id"] == cm_id]
                if not rc.empty:
                    langs, is_rev = _suffix_to_lang_rev(suffix)
                    if langs:
                        filtered = rc[rc["lang"].isin(langs)]
                        if is_rev is not None:
                            filtered_rev = filtered[filtered["is_reverse"].astype(str).str.lower() == str(is_rev).lower()]
                            if not filtered_rev.empty:
                                filtered = filtered_rev
                        row = filtered.iloc[0] if not filtered.empty else rc.iloc[0]
                    else:
                        row = rc.iloc[0]
                    ref_product = {
                        "display_name":   row.get("card_name", sku),
                        "language":       row.get("lang", ""),
                        "business_rarity": row.get("rarity", ""),
                    }

        if sku not in catalog.index and ref_product is None:
            return False, f"SKU no encontrado: {sku}"

    product = ref_product if ref_product is not None else catalog.loc[sku]
    scan_mode = st.session_state.get("scan_mode", "venta")
    if scan_mode == "venta":
        payment_method  = st.session_state.get("payment_mode", "efectivo")
        money_direction = "ninguno"
    elif st.session_state.get("cambio_has_money", False):
        payment_method  = st.session_state.get("payment_mode", "efectivo")
        money_direction = st.session_state.get("cambio_direction", "pagar")
    else:
        payment_method  = "ninguno"
        money_direction = "ninguno"
    save_sale({
        "sale_event_id":  str(uuid.uuid4()),
        "sale_ts":        datetime.now(TZ_MADRID).isoformat(timespec="seconds"),
        "session_id":     st.session_state.get("current_session_id", ""),
        "internal_sku":   sku,
        "display_name":   product["display_name"],
        "language":       product["language"],
        "business_rarity": product["business_rarity"],
        "qty":            1,
        "unit_price":     0.0,
        "gross_amount":   0.0,
        "discount_eur":   0.0,
        "channel":        "physical_store",
        "source_system":  "store_scan",
        "status":         "completed",
        "sale_type":      scan_mode,
        "payment_method": payment_method,
        "money_direction": money_direction,
        "trade_amount":   0.0,
    })
    return True, f"{product['display_name']} · {product['language']} · {product['business_rarity']}"


def toggle_sale_type(sale_event_id: str) -> None:
    """Alterna sale_type entre 'venta' y 'cambio' en session_state."""
    for i, s in enumerate(st.session_state.sales):
        if s.get("sale_event_id") == sale_event_id:
            current = s.get("sale_type", "venta")
            st.session_state.sales[i]["sale_type"] = "cambio" if current == "venta" else "venta"
            break


def void_sale(original: dict) -> None:
    """
    Anula una venta específica añadiendo fila void (nunca edita la original).
    """
    save_sale({
        "sale_event_id":  str(uuid.uuid4()),
        "sale_ts":        datetime.now(TZ_MADRID).isoformat(timespec="seconds"),
        "internal_sku":   str(original["internal_sku"]),
        "display_name":   original.get("display_name", ""),
        "language":       original["language"],
        "business_rarity": original["business_rarity"],
        "qty":            1,
        "unit_price":     0.0,
        "gross_amount":   0.0,
        "channel":        "physical_store",
        "source_system":  "store_scan",
        "status":         "void",
        "sale_type":      original.get("sale_type", "venta"),
        "payment_method": original.get("payment_method", "ninguno"),
        "money_direction": original.get("money_direction", "ninguno"),
        "trade_amount":   original.get("trade_amount", 0.0),
        "discount_eur":   original.get("discount_eur", 0.0),
        "session_id":     original.get("session_id", ""),
    })


# ─────────────────────────────────────────────
# ETIQUETAS — funciones auxiliares
# ─────────────────────────────────────────────
import io as _io
import re as _re

from reportlab.pdfgen import canvas as _canvas
from reportlab.lib.units import mm as _mm
from reportlab.lib.colors import black as _black, white as _white, HexColor as _HexColor
from reportlab.pdfbase.pdfmetrics import stringWidth as _stringWidth
from reportlab.pdfbase.ttfonts import TTFont as _TTFont
from reportlab.pdfbase import pdfmetrics as _pdfmetrics
import barcode as _barcode

_LBL_W  = 60 * _mm
_LBL_H  = 30 * _mm
_TOP_H  = 13 * _mm
_LEFT_W = 18 * _mm
_BC_H   = 15 * _mm
_PAD    =  1 * _mm

_LANG_MAP_FULL = {
    "English": "ENG", "Spanish": "ESP", "Korean": "KOR",
    "Japanese": "JPN", "French": "FRA", "German": "DEU",
    "Italian": "ITA", "Portuguese": "POR",
}
_LANGS_OCC_SET = {"ENG", "ESP", "FRA", "DEU", "ITA", "POR"}


@st.cache_resource
def _register_fonts():
    """Registra fuentes Windows para las etiquetas PDF. Silencia errores si no están."""
    for name, path in [
        ("GothicBold", "C:/Windows/Fonts/GOTHICB.TTF"),
        ("ArialBd",    "C:/Windows/Fonts/arialbd.ttf"),
    ]:
        try:
            _pdfmetrics.registerFont(_TTFont(name, path))
        except Exception:
            pass
    return True


@st.cache_data(ttl=3600)
def _load_release_dates() -> dict:
    """Devuelve dict {set_code: datetime} de ambos ficheros de fechas."""
    import csv as _csv
    dates: dict = {}
    for fpath, sep in [
        (Path("data/Release dates.csv"),    ";"),
        (Path("data/Release dates jp.csv"), ";"),
    ]:
        if not fpath.exists():
            continue
        try:
            with open(fpath, encoding="latin-1") as f:
                reader = _csv.reader(f, delimiter=sep)
                for row in reader:
                    if len(row) < 2:
                        continue
                    cell = row[0].strip()
                    # La última palabra del campo Name es el set_code (e.g. "Ascended Heroes ASC")
                    parts = cell.split()
                    if not parts:
                        continue
                    code = parts[-1].upper()
                    date_str = row[1].strip()
                    try:
                        from datetime import datetime as _dt
                        # Soportar YYYY-MM-DD y el antiguo formato "01 Jan 25"
                        for fmt in ("%Y-%m-%d", "%d %b %y", "%d %b %Y"):
                            try:
                                d = _dt.strptime(date_str, fmt)
                                dates[code] = d
                                break
                            except ValueError:
                                continue
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass
    return dates


def _label_sort_key(entry: dict, release_dates: dict) -> tuple:
    """Clave de ordenación: grupo(occ/jp) → fecha desc → cn numérico."""
    lang = entry.get("lang", "")
    group = 0 if lang in _LANGS_OCC_SET else 1
    sc = entry.get("set_code", "").upper()
    d = release_dates.get(sc)
    date_ts = -d.timestamp() if d else 1e12
    m = _re.search(r"(\d+)", entry.get("cn", ""))
    cn_num = int(m.group(1)) if m else 9999
    return (group, date_ts, cn_num)


def _draw_label(c, data: dict):
    """Dibuja una etiqueta de 60×30mm en el canvas dado."""
    _register_fonts()
    W, H = _LBL_W, _LBL_H
    top_y = H - _TOP_H

    # Fondo blanco + borde
    c.setFillColor(_white)
    c.setStrokeColor(_black)
    c.setLineWidth(0.8)
    c.rect(0, 0, W, H, fill=1, stroke=1)

    # Bloque negro izquierdo (PRISMA)
    c.setFillColor(_black)
    c.rect(0, top_y, _LEFT_W, _TOP_H, fill=1, stroke=0)

    blk_pad = 1.5 * _mm
    blk_w   = _LEFT_W - 2 * blk_pad
    font = "GothicBold" if "GothicBold" in _pdfmetrics.getRegisteredFontNames() else "Helvetica-Bold"

    fs = 24.0
    while _stringWidth("PRISMA", font, fs) > blk_w and fs > 6:
        fs -= 0.5
    c.setFillColor(_white)
    c.setFont(font, fs)
    pw = _stringWidth("PRISMA", font, fs)
    c.drawString((_LEFT_W - pw) / 2, top_y + _TOP_H * 0.50, "PRISMA")

    sub_fs = fs * 0.48
    while _stringWidth("COLLECT & PLAY!", font, sub_fs) > blk_w and sub_fs > 2:
        sub_fs -= 0.25
    c.setFont(font, sub_fs)
    sw = _stringWidth("COLLECT & PLAY!", font, sub_fs)
    sx = (_LEFT_W - sw) / 2
    sy = top_y + _TOP_H * 0.24
    for dx, dy in [(0, 0), (0.3, 0), (0, 0.3), (0.3, 0.3)]:
        c.drawString(sx + dx, sy + dy, "COLLECT & PLAY!")

    c.setStrokeColor(_black)
    c.setLineWidth(0.6)
    c.line(_LEFT_W, top_y, _LEFT_W, H)

    right_x = _LEFT_W + 2 * _PAD
    right_w  = W - _LEFT_W - 2 * _PAD

    c.setFillColor(_black)
    name_str = f"{data['name']} ({data['lang']})"
    fs1 = 9.0
    while _stringWidth(name_str, "Helvetica-BoldOblique", fs1) > right_w and fs1 > 5:
        fs1 -= 0.5
    c.setFont("Helvetica-BoldOblique", fs1)
    c.drawString(right_x, top_y + _TOP_H * 0.65, name_str)

    c.setFont("Helvetica-Bold", 8)
    c.drawString(right_x, top_y + _TOP_H * 0.38, f"{data['set_code']} - {data['cn']}")

    # Línea 3: SKU + badge de condición
    cond = str(data.get("condition", "")).strip().upper()
    c.setFont("Helvetica-Bold", 8)
    c.drawString(right_x, top_y + _TOP_H * 0.10, data["sku"])
    if cond:
        badge_fs = 6.5
        badge_txt = f"[{cond}]"
        badge_w = _stringWidth(badge_txt, "Helvetica-Bold", badge_fs) + 2 * _mm
        badge_h = badge_fs * 1.4
        bx = W - _PAD - badge_w - 0.5 * _mm
        by = top_y + _TOP_H * 0.04
        if cond == "NM":
            c.setFillColor(_HexColor("#888888"))
        else:
            c.setFillColor(_HexColor("#c00000"))
        c.roundRect(bx, by, badge_w, badge_h, 1 * _mm, fill=1, stroke=0)
        c.setFillColor(_white)
        c.setFont("Helvetica-Bold", badge_fs)
        c.drawString(bx + 1 * _mm, by + badge_h * 0.25, badge_txt)

    c.setStrokeColor(_black)
    c.setLineWidth(0.6)
    c.line(0, top_y, W, top_y)

    # Barcode using python-barcode library
    bc_y = (top_y - _BC_H) / 2
    bc_margin_x = 2 * _mm
    bc_width = W - 2 * bc_margin_x
    bc_height = _BC_H - 1 * _mm
    try:
        bc_sku = str(data.get("sku", "")).strip()
        if bc_sku:
            # Generate barcode as PNG image in memory
            bc_gen = _barcode.get("code128", bc_sku, module_height=2.0)
            bc_img_buf = _io.BytesIO()
            bc_gen.save(bc_img_buf, format="png")
            bc_img_buf.seek(0)
            # Draw barcode image on canvas
            c.drawImage(bc_img_buf, bc_margin_x, bc_y,
                       width=bc_width, height=bc_height,
                       preserveAspectRatio=True, anchor='sw')
        else:
            # DEBUG: RED rect if no SKU
            c.setFillColor(_HexColor("#FF0000"))
            c.rect(bc_margin_x, bc_y, bc_width, bc_height, fill=1, stroke=0)
    except Exception as e:
        # DEBUG: BLUE rect if barcode generation/draw fails
        c.setFillColor(_HexColor("#0000FF"))
        c.rect(bc_margin_x, bc_y, bc_width, bc_height, fill=1, stroke=0)


def _generate_label_pdf(labels: list) -> bytes:
    """Genera el PDF con todas las etiquetas y devuelve los bytes."""
    buf = _io.BytesIO()
    c = _canvas.Canvas(buf, pagesize=(_LBL_W, _LBL_H))
    for i, data in enumerate(labels):
        _draw_label(c, data)
        if i < len(labels) - 1:
            c.showPage()
    c.save()
    return buf.getvalue()


@st.cache_data
def _build_cm_index() -> dict:
    """Construye índice (cardmarket_id_str, lang_short) → internal_sku desde el catálogo."""
    cat = load_catalog()
    idx: dict = {}
    if "cardmarket_id" not in cat.columns or "language" not in cat.columns:
        return idx
    for sku, row in cat.iterrows():
        key = (str(row["cardmarket_id"]).strip(), str(row["language"]).strip())
        idx[key] = sku
    return idx


def _parse_labels_from_csv(file_bytes: bytes, cm_idx: dict) -> tuple[list, list]:
    """
    Parsea CSV de Cardmarket (o con internal_sku directo).
    Devuelve (labels_list, unmatched_rows).
    Cada label: {sku, name, lang, set_code, cn, condition}.
    """
    import csv as _csv
    content = file_bytes.decode("utf-8-sig")
    reader = _csv.DictReader(_io.StringIO(content))
    labels_raw: list = []
    unmatched: list  = []
    ref_cards_idx = {}
    if not ref_cards_df.empty:
        # Índice de ref_cards por cardmarket_id + lang
        for _, rc in ref_cards_df.iterrows():
            cm_id = str(rc.get("cardmarket_id", "")).strip()
            lang = str(rc.get("lang", "")).strip()
            if cm_id and lang:
                key = (cm_id, lang)
                if key not in ref_cards_idx:
                    ref_cards_idx[key] = rc

    for row in reader:
        # Formato con internal_sku directo
        if "internal_sku" in row and row["internal_sku"].strip():
            sku = row["internal_sku"].strip()
            qty = int(row.get("qty", row.get("quantity", 1)) or 1)
            labels_raw.append({
                "sku":      sku,
                "name":     row.get("card_name", row.get("name", sku)),
                "lang":     row.get("lang", row.get("language", "")),
                "set_code": row.get("set_code", row.get("setCode", "")),
                "cn":       row.get("cn", ""),
                "condition": row.get("condition", ""),
                "_qty":     qty,
            })
            continue

        # Formato Cardmarket export (cardmarketId + language + setCode + cn + condition)
        lang_full = row.get("language", "").strip()
        lang = _LANG_MAP_FULL.get(lang_full, lang_full)
        cm_id = str(row.get("cardmarketId", row.get("cardmarket_id", ""))).strip()
        qty = int(row.get("quantity", 1) or 1)

        # 1. Buscar en inventory_current (cm_idx)
        sku = cm_idx.get((cm_id, lang))

        # 2. Fallback: buscar en ref_cards si no está en inventory
        ref_info = None
        if not sku and (cm_id, lang) in ref_cards_idx:
            ref_info = ref_cards_idx[(cm_id, lang)]
            # En ref_cards no hay internal_sku, usar cardmarketId + 4-digit suffix
            # (mismo patrón que en register_scan)
            sku = f"{cm_id}-0002" if lang == "ENG" else f"{cm_id}-0001"

        if not sku:
            unmatched.append(f"{row.get('name','?')} ({lang}) [cardmarketId: {cm_id}]")
            sku = cm_id  # fallback final: al menos el número de barcode

        labels_raw.append({
            "sku":      sku,
            "name":     row.get("name", ""),
            "lang":     lang,
            "set_code": row.get("setCode", row.get("set_code", "")).upper(),
            "cn":       row.get("cn", ""),
            "condition": row.get("condition", ""),
            "_qty":     qty,
        })

    release_dates = _load_release_dates()
    labels_raw.sort(key=lambda e: _label_sort_key(e, release_dates))

    labels: list = []
    for entry in labels_raw:
        qty = entry.pop("_qty", 1)
        for _ in range(qty):
            labels.append(dict(entry))
    return labels, unmatched


# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "last_msg" not in st.session_state:
    st.session_state.last_msg = None
if "last_ok" not in st.session_state:
    st.session_state.last_ok = True
# Contador para resetear el campo de escaneo tras cada registro exitoso
if "scan_counter" not in st.session_state:
    st.session_state.scan_counter = 0
if "scan_mode" not in st.session_state:
    st.session_state.scan_mode = "venta"
if "payment_mode" not in st.session_state:
    st.session_state.payment_mode = "efectivo"
if "cambio_has_money" not in st.session_state:
    st.session_state.cambio_has_money = False
if "cambio_direction" not in st.session_state:
    st.session_state.cambio_direction = "pagar"
if "current_session_id" not in st.session_state:
    st.session_state.current_session_id = str(uuid.uuid4())[:8]
if "session_discount" not in st.session_state:
    st.session_state.session_discount = 0.0
if "session_trade_amount" not in st.session_state:
    st.session_state.session_trade_amount = 0.0
if "session_discounts" not in st.session_state:
    st.session_state.session_discounts = {}   # {session_id: discount_eur total del ticket}
if "cambio_amount" not in st.session_state:
    st.session_state.cambio_amount = 0.0

# ─────────────────────────────────────────────
# CATÁLOGO
# ─────────────────────────────────────────────
catalog = load_catalog()
ref_cards_df = load_ref_cards()

# Ventas del día en memoria — carga única desde Sheets/CSV al arrancar.
# Todos los scans y voids del día se añaden aquí directamente (sin red).
if "sales" not in st.session_state:
    df_init = load_daily_sales()
    st.session_state.sales = df_init.to_dict("records")

# ─────────────────────────────────────────────
# A) HEADER
# ─────────────────────────────────────────────
st.markdown(
    """
<div class="prisma-header">
    <p class="prisma-logo">⚡ PRISMA SCAN</p>
    <p class="prisma-subtitle">Registro de ventas · Punto de venta físico</p>
</div>
""",
    unsafe_allow_html=True,
)

_tab_scan, _tab_labels = st.tabs(["⚡  Escáner", "🏷️  Etiquetas"])
_tab_scan.__enter__()   # todo el contenido siguiente va al tab de escáner

# ─────────────────────────────────────────────
# B) COMPONENTE DE ESCANEO
#
# Componente custom con JS propio. Detecta la velocidad de tecleo:
#   - Escáner: ~5-15 ms entre caracteres → dispara solo al terminar
#   - Humano: >80 ms → dispara tras 180 ms de inactividad
# Mantiene el foco automáticamente tras cada rerun.
# La key cambia con scan_counter para reiniciar el componente tras cada scan.
# ─────────────────────────────────────────────
scanned = _scanner_input(key=f"scanner_{st.session_state.scan_counter}")

if scanned:
    sku = str(scanned).strip().upper().replace("/", "-")
    ok, msg = register_scan(sku)
    st.session_state.last_msg = msg
    st.session_state.last_ok  = ok
    st.session_state.scan_counter += 1
    st.rerun()

# ─────────────────────────────────────────────
# B1) SELECTOR DE MODO — VENTA o CAMBIO (sticky)
# ─────────────────────────────────────────────
mode = st.session_state.scan_mode
col_v, col_c = st.columns(2)
if col_v.button(
    "💰  VENTA",
    use_container_width=True,
    type="primary" if mode == "venta" else "secondary",
    key="mode_venta",
):
    st.session_state.scan_mode = "venta"
    st.rerun()
if col_c.button(
    "🔄  CAMBIO",
    use_container_width=True,
    type="primary" if mode == "cambio" else "secondary",
    key="mode_cambio",
):
    st.session_state.scan_mode = "cambio"
    st.rerun()

# ─────────────────────────────────────────────
# B2) PAGO — condicional según modo VENTA / CAMBIO
# ─────────────────────────────────────────────
if st.session_state.scan_mode == "venta":
    # VENTA: solo canal de cobro
    pay = st.session_state.payment_mode
    col_ef, col_tj = st.columns(2)
    if col_ef.button("💵  EFECTIVO", use_container_width=True,
                     type="primary" if pay == "efectivo" else "secondary", key="mode_efectivo"):
        st.session_state.payment_mode = "efectivo"; st.rerun()
    if col_tj.button("💳  TARJETA", use_container_width=True,
                     type="primary" if pay == "tarjeta" else "secondary", key="mode_tarjeta"):
        st.session_state.payment_mode = "tarjeta"; st.rerun()
else:
    # CAMBIO: primero si hay dinero de por medio
    has_money = st.session_state.cambio_has_money
    col_dir, col_mon = st.columns(2)
    if col_dir.button("🔄  DIRECTO", use_container_width=True,
                      type="secondary" if has_money else "primary", key="cambio_directo"):
        st.session_state.cambio_has_money = False; st.rerun()
    if col_mon.button("💸  CON DINERO", use_container_width=True,
                      type="primary" if has_money else "secondary", key="cambio_dinero"):
        st.session_state.cambio_has_money = True; st.rerun()
    if has_money:
        direction = st.session_state.cambio_direction
        col_pag, col_rec = st.columns(2)
        if col_pag.button("📤  A PAGAR", use_container_width=True,
                          type="primary" if direction == "pagar" else "secondary", key="dir_pagar"):
            st.session_state.cambio_direction = "pagar"; st.rerun()
        if col_rec.button("📥  A RECIBIR", use_container_width=True,
                          type="primary" if direction == "recibir" else "secondary", key="dir_recibir"):
            st.session_state.cambio_direction = "recibir"; st.rerun()
        pay = st.session_state.payment_mode
        col_ef2, col_tj2 = st.columns(2)
        if col_ef2.button("💵  EFECTIVO", use_container_width=True,
                          type="primary" if pay == "efectivo" else "secondary", key="mode_efectivo"):
            st.session_state.payment_mode = "efectivo"; st.rerun()
        if col_tj2.button("💳  TARJETA", use_container_width=True,
                          type="primary" if pay == "tarjeta" else "secondary", key="mode_tarjeta"):
            st.session_state.payment_mode = "tarjeta"; st.rerun()
        # Campo importe del cambio
        _tamt = st.number_input(
            "Importe cambio (€)", min_value=0.0, max_value=9999.0,
            value=st.session_state.session_trade_amount,
            step=0.50, format="%.2f", key="session_trade_input",
        )
        st.session_state.session_trade_amount = _tamt

# ─────────────────────────────────────────────
# B3) ENTRADA MANUAL — por si falla una etiqueta
# ─────────────────────────────────────────────
with st.expander("Entrada manual (etiqueta dañada)"):
    # Separar expansiones occidentales y japonesas/coreanas por idioma
    LANGS_JP = {"JPN", "KOR"}
    LANGS_OCC = {"ENG", "ESP", "FRA", "DEU", "ITA", "POR"}

    cat_reset = catalog.reset_index()
    _sets_occ = (
        cat_reset[cat_reset["language"].isin(LANGS_OCC)]["set_code"]
        .drop_duplicates().sort_values().tolist()
        if "language" in cat_reset.columns and "set_code" in cat_reset.columns else []
    )
    _sets_jp = (
        cat_reset[cat_reset["language"].isin(LANGS_JP)]["set_code"]
        .drop_duplicates().sort_values().tolist()
        if "language" in cat_reset.columns and "set_code" in cat_reset.columns else []
    )

    NONE_OPT = "—"
    col_occ, col_jp = st.columns(2)
    with col_occ:
        sel_occ = st.selectbox("Expansión occidental", options=[NONE_OPT] + _sets_occ)
    with col_jp:
        sel_jp  = st.selectbox("Expansión japonesa / coreana", options=[NONE_OPT] + _sets_jp)

    # El set activo es el que no sea "—" (si ambos están rellenos, occidental tiene preferencia)
    manual_set = sel_occ if sel_occ != NONE_OPT else (sel_jp if sel_jp != NONE_OPT else None)

    col_b, col_c = st.columns([1.0, 1.0])
    with col_b:
        lang_options = sorted(catalog["language"].dropna().unique().tolist()) if "language" in catalog.columns else []
        manual_lang = st.selectbox("Idioma", options=lang_options, label_visibility="visible")
    with col_c:
        manual_cn = st.text_input("Nº carta (cn)", placeholder="001", label_visibility="visible")

    if st.button("Registrar manual", use_container_width=True, type="primary"):
        if manual_set and manual_lang and manual_cn:
            mask = (
                (catalog.get("set_code", pd.Series(dtype=str)) == manual_set) &
                (catalog.get("language", pd.Series(dtype=str)) == manual_lang) &
                (catalog.get("cn", pd.Series(dtype=str)) == manual_cn.strip())
            )
            matches = catalog[mask]
            if not matches.empty:
                sku_manual = matches.index[0]
                ok, msg = register_scan(sku_manual)
                st.session_state.last_msg = msg
                st.session_state.last_ok  = ok
                st.rerun()
            else:
                st.session_state.last_msg = f"No encontrado: {manual_set} · {manual_lang} · {manual_cn}"
                st.session_state.last_ok  = False
                st.rerun()
        else:
            st.warning("Rellena los tres campos.")

# ─────────────────────────────────────────────
# B5) BUSCADOR DE INVENTARIO
# ─────────────────────────────────────────────
with st.expander("🔍 Buscar carta en inventario"):
    inv = catalog.reset_index()
    # Solo cartas con stock disponible
    if "qty" in inv.columns:
        inv = inv[pd.to_numeric(inv["qty"], errors="coerce").fillna(0) > 0]

    if inv.empty:
        st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Inventario no disponible</span>', unsafe_allow_html=True)
    else:
        search_name = st.text_input("Nombre Pokémon", placeholder="Snorlax...", key="search_name")
        s1, s2, s3 = st.columns(3)
        with s1:
            set_opts = ["Todas"] + sorted(inv["set_name"].dropna().unique().tolist())
            search_set = st.selectbox("Expansión", set_opts, key="search_set")
        with s2:
            lang_opts = ["Todos"] + sorted(inv["language"].dropna().unique().tolist())
            search_lang = st.selectbox("Idioma", lang_opts, key="search_lang")
        with s3:
            rar_opts = ["Todas"] + sorted(inv["business_rarity"].dropna().unique().tolist())
            search_rar = st.selectbox("Rareza", rar_opts, key="search_rar")

        has_filter = search_name or search_set != "Todas" or search_lang != "Todos" or search_rar != "Todas"
        if has_filter:
            mask = pd.Series(True, index=inv.index)
            if search_name:
                mask &= inv["display_name"].str.contains(search_name, case=False, na=False)
            if search_set != "Todas":
                mask &= inv["set_name"] == search_set
            if search_lang != "Todos":
                mask &= inv["language"] == search_lang
            if search_rar != "Todas":
                mask &= inv["business_rarity"] == search_rar
            res = inv[mask][["internal_sku", "display_name", "language", "business_rarity", "set_name", "cn"]].rename(columns={
                "display_name": "nombre", "language": "idioma", "business_rarity": "rareza", "set_name": "expansión",
            })
            if res.empty:
                st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Sin resultados</span>', unsafe_allow_html=True)
            else:
                st.dataframe(res.head(100), use_container_width=True, hide_index=True)
                st.markdown(f'<span style="color:var(--prisma-muted);font-size:0.72rem;">{len(res)} resultado(s)</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Introduce al menos un filtro para buscar</span>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# C) FEEDBACK DEL ÚLTIMO ESCANEO + ERRORES SHEETS
# ─────────────────────────────────────────────
if st.session_state.get("sheets_error"):
    st.error(f"⚠️ Error al guardar en Sheets: {st.session_state['sheets_error']}")

if st.session_state.last_msg:
    css = "alert-ok" if st.session_state.last_ok else "alert-error"
    icon = "✅" if st.session_state.last_ok else "❌"
    st.markdown(
        f'<div class="alert {css}">{icon} {st.session_state.last_msg}</div>',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
# C2) TICKET DE COMPRA — sesión actual
# ─────────────────────────────────────────────
st.markdown('<p class="summary-title" style="margin:0.6rem 0 0.2rem 0;">🧾 Ticket actual</p>', unsafe_allow_html=True)

sess_id = st.session_state.current_session_id
sess_sales = [s for s in st.session_state.sales
              if s.get("session_id") == sess_id and s.get("status") == "completed"]

if not sess_sales:
    st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Sin artículos — escanea para añadir</span>', unsafe_allow_html=True)
else:
    df_sess = pd.DataFrame(sess_sales)
    grp_sess = df_sess.groupby(
        ["internal_sku", "display_name", "language", "business_rarity"],
        as_index=False
    )["qty"].sum().sort_values("display_name")
    for _, row in grp_sess.iterrows():
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:5px 0;border-bottom:1px solid var(--prisma-border);">'
            f'<span style="font-size:0.82rem;"><b>{row["display_name"]}</b> '
            f'<span style="color:var(--prisma-muted);">· {row["language"]} · {row["business_rarity"]}</span></span>'
            f'<span style="font-family:JetBrains Mono,monospace;color:var(--prisma-accent);font-weight:700;">×{int(row["qty"])}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    total_u = int(df_sess["qty"].sum())

    # ¿Hay cambios con dinero en esta sesión?
    has_cambio_money = any(
        s.get("sale_type") == "cambio" and s.get("money_direction") in ("pagar", "recibir")
        for s in sess_sales
    )

    disc_val = st.number_input(
        "Descuento (€)", min_value=0.0, max_value=9999.0,
        value=st.session_state.session_discount,
        step=0.50, format="%.2f", key="session_discount_input",
    )
    st.session_state.session_discount = disc_val
    tamt_val = st.session_state.session_trade_amount

    extras = []
    if disc_val > 0:
        extras.append(f'<span style="color:var(--prisma-danger);">-{disc_val:.2f}€ dto.</span>')
    if tamt_val > 0:
        extras.append(f'<span style="color:var(--prisma-info);">{tamt_val:.2f}€ cambio</span>')
    st.markdown(
        f'<div style="margin-top:4px;font-size:0.78rem;color:var(--prisma-muted);text-align:right;">'
        f'{total_u} carta(s){"  ·  ".join([""] + extras) if extras else ""}</div>',
        unsafe_allow_html=True,
    )

# Callback "Nuevo ticket" — se ejecuta antes del siguiente render, por eso puede limpiar keys de widgets
def _nuevo_ticket():
    sid         = st.session_state.current_session_id
    disc_final  = st.session_state.session_discount
    trade_final = st.session_state.session_trade_amount
    if disc_final > 0:
        st.session_state.session_discounts[sid] = disc_final
    # Estampar trade_amount solo en la primera carta de cambio con dinero (importe es del ticket, no por carta)
    if trade_final > 0:
        stamped = False
        for i, s in enumerate(st.session_state.sales):
            if (s.get("session_id") == sid
                    and s.get("money_direction") in ("pagar", "recibir")):
                st.session_state.sales[i]["trade_amount"] = trade_final if not stamped else 0.0
                stamped = True
    st.session_state.current_session_id   = str(uuid.uuid4())[:8]
    st.session_state.session_discount     = 0.0
    st.session_state.session_trade_amount = 0.0
    st.session_state.cambio_has_money     = False
    # Borrar keys de widgets para que los inputs se reinicien al siguiente render
    st.session_state.pop("session_discount_input", None)
    st.session_state.pop("session_trade_input", None)

st.button("➕ Nuevo ticket", key="new_ticket", use_container_width=True,
          type="secondary", on_click=_nuevo_ticket)

# ─────────────────────────────────────────────
# D) DOS LISTAS: VENTAS | CAMBIOS
# ─────────────────────────────────────────────
df_sales = pd.DataFrame(st.session_state.sales, columns=CSV_COLUMNS) if st.session_state.sales else pd.DataFrame(columns=CSV_COLUMNS)

st.markdown('<p class="summary-title">Registro de hoy</p>', unsafe_allow_html=True)

if df_sales.empty:
    st.markdown(
        '<div class="alert alert-warn" style="text-align:center; margin-top:0.5rem;">'
        "Sin registros hoy — esperando primer escaneo</div>",
        unsafe_allow_html=True,
    )
else:
    df_active = df_sales[df_sales["status"] == "completed"]
    df_void   = df_sales[df_sales["status"] == "void"]
    voided_ids = set(df_void["internal_sku"].tolist())

    if "sale_type" not in df_active.columns:
        df_active = df_active.copy()
        df_active["sale_type"] = "venta"
    df_ventas  = df_active[df_active["sale_type"] == "venta"]
    df_cambios = df_active[df_active["sale_type"] == "cambio"]

    n_ventas  = len(df_ventas)
    n_cambios = len(df_cambios)
    n_voided  = len(df_void) // 2  # cada void cancela una completed

    # Métricas
    col1, col2, col3 = st.columns(3)
    col1.markdown(
        f'<div class="metric-card"><p class="metric-value" style="color:#4cdf80;">{n_ventas}</p>'
        '<p class="metric-label">💰 Ventas</p></div>', unsafe_allow_html=True)
    col2.markdown(
        f'<div class="metric-card"><p class="metric-value" style="color:#5badff;">{n_cambios}</p>'
        '<p class="metric-label">🔄 Cambios</p></div>', unsafe_allow_html=True)
    col3.markdown(
        f'<div class="metric-card"><p class="metric-value" style="color:var(--prisma-muted);">{n_ventas + n_cambios}</p>'
        '<p class="metric-label">Total neto</p></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Lista activa según el modo seleccionado
    modo_activo = st.session_state.scan_mode
    if modo_activo == "venta":
        df_lista, color, prefix, label = df_ventas, "#4cdf80", "v", "💰 Ventas"
    else:
        df_lista, color, prefix, label = df_cambios, "#5badff", "c", "🔄 Cambios"

    st.markdown(
        f'<p style="color:{color};font-weight:700;font-size:0.8rem;'
        f'text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px;">{label}</p>',
        unsafe_allow_html=True,
    )

    if df_lista.empty:
        st.markdown(
            f'<span style="color:var(--prisma-muted);font-size:0.8rem;">Sin {label.lower()} registradas aún</span>',
            unsafe_allow_html=True,
        )
    else:
        # Scroll a partir de la entrada 10 (altura fija ~450px ≈ 10 entradas)
        scroll_h = 450 if len(df_lista) > 10 else None
        container = st.container(height=scroll_h) if scroll_h else st.container()
        with container:
            for i, (_, row) in enumerate(df_lista.iloc[::-1].iterrows()):
                try:
                    t = datetime.fromisoformat(str(row["sale_ts"])).strftime("%H:%M")
                except (ValueError, TypeError):
                    t = "—"

                # Obtener set_code y cn del catálogo
                sku = row["internal_sku"]
                set_info = ""
                if sku in catalog.index:
                    cat_row = catalog.loc[sku]
                    sc = cat_row.get("set_code", "") if hasattr(cat_row, "get") else ""
                    cn = cat_row.get("cn", "") if hasattr(cat_row, "get") else ""
                    if sc or cn:
                        set_info = f" · {sc} {cn}".strip()

                c1, c2 = st.columns([5, 1])
                c1.markdown(
                    f'<div style="border-left:3px solid {color};padding-left:8px;margin-bottom:4px;">'
                    f'<span style="font-size:0.85rem;font-weight:600;">{row["display_name"]}</span><br>'
                    f'<span style="font-size:0.7rem;color:var(--prisma-muted);">'
                    f'{t} · {row["language"]} · {row["business_rarity"]}{set_info}</span></div>',
                    unsafe_allow_html=True,
                )
                if c2.button("✕", key=f"void_{prefix}_{row['sale_event_id']}"):
                    void_sale(row.to_dict())
                    st.session_state.last_msg = f"Anulada: {row['display_name']}"
                    st.session_state.last_ok  = True
                    st.rerun()

# ─────────────────────────────────────────────
# E) CERRAR CAJA
# ─────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    '<p style="font-family:\'JetBrains Mono\',monospace;font-size:0.75rem;'
    'letter-spacing:0.12em;color:var(--prisma-muted);text-transform:uppercase;'
    'margin-bottom:0.5rem;">CERRAR CAJA</p>',
    unsafe_allow_html=True,
)

if not df_sales.empty:
    df_completed = df_sales[df_sales["status"] == "completed"].copy()
    df_void      = df_sales[df_sales["status"] == "void"].copy()

    # ── Resumen de sesión ──────────────────────────────────────────────
    if not df_completed.empty:
        _qty_total   = pd.to_numeric(df_completed.get("qty", 0), errors="coerce").fillna(0).sum()
        _gross_total = pd.to_numeric(df_completed.get("gross_amount", 0), errors="coerce").fillna(0).sum()
        _disc_total  = pd.to_numeric(df_completed.get("discount_eur", 0), errors="coerce").fillna(0).sum()
        _net_total   = _gross_total - _disc_total

        # Desglose por método de pago
        _pm = df_completed.copy()
        _pm["gross_amount"] = pd.to_numeric(_pm.get("gross_amount", 0), errors="coerce").fillna(0)
        _efect = _pm[_pm.get("payment_method", pd.Series()) == "efectivo"]["gross_amount"].sum() if "payment_method" in _pm.columns else 0.0
        _tarj  = _pm[_pm.get("payment_method", pd.Series()) == "tarjeta"]["gross_amount"].sum()  if "payment_method" in _pm.columns else 0.0

        # Cambios (trades)
        _cambios = _pm[_pm.get("money_direction", pd.Series()) == "cambio"]["gross_amount"].sum() if "money_direction" in _pm.columns else 0.0

        _disc_row = (
            f'<div style="display:flex;justify-content:space-between;margin-bottom:0.6rem;">'
            f'<span style="color:var(--prisma-muted);font-size:0.8rem;">Descuentos</span>'
            f'<span style="color:#f04060;font-size:0.9rem;">−{_disc_total:.2f} €</span></div>'
        ) if _disc_total > 0 else ""
        _cambios_span = (
            f'<span style="font-size:0.75rem;color:var(--prisma-muted);">🔄 Cambios: <b>{_cambios:.2f} €</b></span>'
        ) if _cambios > 0 else ""
        st.markdown(
            f'<div style="background:var(--prisma-surface);border:1px solid var(--prisma-border);'
            f'border-radius:12px;padding:1rem 1.2rem;margin-bottom:1rem;">'
            f'<div style="display:flex;justify-content:space-between;margin-bottom:0.6rem;">'
            f'<span style="color:var(--prisma-muted);font-size:0.8rem;">Cartas vendidas</span>'
            f'<span style="font-weight:700;font-size:0.9rem;">{int(_qty_total)} uds</span></div>'
            f'<div style="display:flex;justify-content:space-between;margin-bottom:0.6rem;">'
            f'<span style="color:var(--prisma-muted);font-size:0.8rem;">Total bruto</span>'
            f'<span style="font-weight:700;font-size:0.9rem;">{_gross_total:.2f} €</span></div>'
            f'{_disc_row}'
            f'<div style="border-top:1px solid var(--prisma-border);margin:0.5rem 0;"></div>'
            f'<div style="display:flex;justify-content:space-between;margin-bottom:0.6rem;">'
            f'<span style="color:var(--prisma-accent);font-size:0.9rem;font-weight:700;">NETO</span>'
            f'<span style="color:var(--prisma-accent);font-weight:700;font-size:1rem;">{_net_total:.2f} €</span></div>'
            f'<div style="display:flex;gap:1rem;margin-top:0.4rem;">'
            f'<span style="font-size:0.75rem;color:var(--prisma-muted);">💵 Efectivo: <b>{_efect:.2f} €</b></span>'
            f'<span style="font-size:0.75rem;color:var(--prisma-muted);">💳 Tarjeta: <b>{_tarj:.2f} €</b></span>'
            f'{_cambios_span}'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── Preparar CSV VSA ──────────────────────────────────────────────
    grp_cols = ["session_id", "internal_sku", "display_name", "language", "business_rarity",
                "unit_price", "channel", "source_system", "status",
                "sale_type", "payment_method", "money_direction", "trade_amount", "discount_eur"]
    grp_cols = [c for c in grp_cols if c in df_completed.columns]
    if not df_completed.empty:
        df_agg = df_completed.groupby(grp_cols, as_index=False).agg({"qty": "sum", "gross_amount": "sum"})
        df_agg["sale_event_id"] = df_agg["session_id"] + "-" + df_agg["internal_sku"]
        df_agg["sale_ts"]       = datetime.now(TZ_MADRID).isoformat(timespec="seconds")
        df_export = pd.concat([df_agg, df_void], ignore_index=True)
    else:
        df_export = df_void

    # Aplicar descuento por ticket (una sola vez por session_id)
    disc_map = st.session_state.get("session_discounts", {})
    if disc_map and "session_id" in df_export.columns:
        df_export = df_export.copy()
        seen = set()
        def _apply_disc(row):
            sid = row.get("session_id", "")
            if sid in disc_map and sid not in seen:
                seen.add(sid)
                return disc_map[sid]
            return 0.0
        df_export["discount_eur"] = df_export.apply(_apply_disc, axis=1)

    for col in CSV_COLUMNS:
        if col not in df_export.columns:
            df_export[col] = ""
    df_export = df_export[CSV_COLUMNS]

    vsa_filename = f"VSA_{TODAY}.csv"
    st.download_button(
        label="🔒  CERRAR CAJA — Descargar VSA",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name=vsa_filename,
        mime="text/csv",
        use_container_width=True,
        type="primary",
    )
    st.markdown(
        f'<p style="font-size:0.72rem;color:var(--prisma-muted);text-align:center;">'
        f'Genera <code>{vsa_filename}</code> · Súbelo a Drive → Sant Antoni/ para el pipeline.</p>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<p style="font-size:0.78rem;color:var(--prisma-muted);text-align:center;">'
        "Sin ventas registradas hoy — el botón aparecerá al cerrar el primer ticket.</p>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
# F) FOOTER
# ─────────────────────────────────────────────
st.markdown(
    f'<div class="prisma-footer">Prisma Scan · {TODAY} · v2.0</div>',
    unsafe_allow_html=True,
)

_tab_scan.__exit__(None, None, None)   # fin del tab de escáner

# ─────────────────────────────────────────────
# G) TAB ETIQUETAS
# ─────────────────────────────────────────────
with _tab_labels:
    st.markdown('<p class="summary-title" style="margin-bottom:0.8rem;">Generador de etiquetas</p>', unsafe_allow_html=True)

    _lbl_mode = st.radio(
        "Origen de los datos",
        ["📁  Subir CSV", "🔍  Selección manual"],
        horizontal=True,
        label_visibility="collapsed",
        key="lbl_mode",
    )

    # ── MODO CSV ──────────────────────────────────────────────────────────
    if _lbl_mode == "📁  Subir CSV":
        st.markdown(
            '<span style="font-size:0.8rem;color:var(--prisma-muted);">'
            'Acepta exportaciones de Cardmarket o CSVs con columna <code>internal_sku</code>.</span>',
            unsafe_allow_html=True,
        )
        _uploaded = st.file_uploader("Sube el CSV", type=["csv"], key="lbl_csv_upload")
        if _uploaded is not None:
            try:
                _cm_idx = _build_cm_index()
                _labels, _unmatched = _parse_labels_from_csv(_uploaded.read(), _cm_idx)
                if _unmatched:
                    with st.expander(f"⚠️ {len(_unmatched)} SKU(s) sin coincidir en inventory_current"):
                        for _u in _unmatched:
                            st.markdown(f"- {_u}")
                if _labels:
                    st.markdown(
                        f'<div class="alert alert-ok">{len(_labels)} etiqueta(s) listas · '
                        f'{sum(1 for l in _labels if l.get("condition","").upper() != "NM")} no-NM</div>',
                        unsafe_allow_html=True,
                    )
                    # Preview tabla
                    _prev = pd.DataFrame(_labels)[["sku", "name", "lang", "set_code", "cn", "condition"]].rename(
                        columns={"sku": "internal_sku", "name": "nombre", "lang": "idioma",
                                 "set_code": "set", "condition": "condición"}
                    )
                    st.dataframe(_prev, use_container_width=True, hide_index=True, height=220)
                    _pdf_bytes = _generate_label_pdf(_labels)
                    _fname = f"etiquetas_{_uploaded.name.replace('.csv','')}.pdf"
                    st.download_button(
                        "⬇️  Descargar PDF de etiquetas",
                        data=_pdf_bytes,
                        file_name=_fname,
                        mime="application/pdf",
                        use_container_width=True,
                        type="primary",
                    )
                else:
                    st.markdown('<div class="alert alert-warn">No se encontraron filas válidas en el CSV.</div>', unsafe_allow_html=True)
            except Exception as _e:
                st.error(f"Error al procesar el CSV: {_e}")

    # ── MODO SELECCIÓN MANUAL ─────────────────────────────────────────────
    else:
        _inv_lbl = catalog.reset_index()
        if "qty" in _inv_lbl.columns:
            _inv_lbl = _inv_lbl[pd.to_numeric(_inv_lbl["qty"], errors="coerce").fillna(0) > 0]

        if _inv_lbl.empty:
            st.markdown('<div class="alert alert-warn">Inventario no disponible.</div>', unsafe_allow_html=True)
        else:
            _la, _lb = st.columns(2)
            with _la:
                _lbl_name = st.text_input("Nombre carta / Pokémon", placeholder="Charizard…", key="lbl_name")
            with _lb:
                _lbl_set_opts = ["Todas"] + sorted(_inv_lbl["set_name"].dropna().unique().tolist())
                _lbl_set = st.selectbox("Expansión", _lbl_set_opts, key="lbl_set")
            _lc, _ld = st.columns(2)
            with _lc:
                _lbl_lang_opts = ["Todos"] + sorted(_inv_lbl["language"].dropna().unique().tolist())
                _lbl_lang = st.selectbox("Idioma", _lbl_lang_opts, key="lbl_lang")
            with _ld:
                _lbl_rar_opts = ["Todas"] + sorted(_inv_lbl["business_rarity"].dropna().unique().tolist())
                _lbl_rar = st.selectbox("Rareza", _lbl_rar_opts, key="lbl_rar")

            _mask_lbl = pd.Series(True, index=_inv_lbl.index)
            if _lbl_name:
                _mask_lbl &= _inv_lbl["display_name"].str.contains(_lbl_name, case=False, na=False)
            if _lbl_set != "Todas":
                _mask_lbl &= _inv_lbl["set_name"] == _lbl_set
            if _lbl_lang != "Todos":
                _mask_lbl &= _inv_lbl["language"] == _lbl_lang
            if _lbl_rar != "Todas":
                _mask_lbl &= _inv_lbl["business_rarity"] == _lbl_rar

            _res_lbl = _inv_lbl[_mask_lbl]
            if _res_lbl.empty:
                st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Sin resultados — ajusta los filtros</span>', unsafe_allow_html=True)
            else:
                _has_inv_qty = "qty" in _res_lbl.columns
                _use_inv_qty = st.checkbox(
                    "Usar cantidad de inventario (qty de inventory_current)",
                    value=_has_inv_qty,
                    key="lbl_use_inv_qty",
                    disabled=not _has_inv_qty,
                )
                if not _use_inv_qty:
                    _lbl_qty = st.number_input(
                        "Etiquetas por carta",
                        min_value=1, max_value=50, value=1, step=1, key="lbl_qty",
                    )
                else:
                    _lbl_qty = None  # se leerá por fila

                # Preview — incluir qty si se usa inventario
                _prev_cols = ["internal_sku", "display_name", "language", "business_rarity", "set_name", "cn"]
                if _use_inv_qty and _has_inv_qty:
                    _prev_cols.append("qty")
                _preview_lbl = _res_lbl[_prev_cols].rename(
                    columns={"display_name": "nombre", "language": "idioma",
                             "business_rarity": "rareza", "set_name": "expansión"}
                )
                st.dataframe(_preview_lbl.head(100), use_container_width=True, hide_index=True, height=220)

                if _use_inv_qty and _has_inv_qty:
                    _total_lbl = int(pd.to_numeric(_res_lbl["qty"], errors="coerce").fillna(1).sum())
                else:
                    _total_lbl = len(_res_lbl) * (_lbl_qty or 1)
                st.markdown(
                    f'<span style="color:var(--prisma-muted);font-size:0.72rem;">'
                    f'{len(_res_lbl)} carta(s) → {_total_lbl} etiqueta(s)</span>',
                    unsafe_allow_html=True,
                )

                if st.button("Generar PDF", use_container_width=True, type="primary", key="lbl_gen_manual"):
                    _release_dates = _load_release_dates()
                    _labels_manual = []
                    for _, _r in _res_lbl.iterrows():
                        _entry = {
                            "sku":       _r["internal_sku"],
                            "name":      _r.get("display_name", ""),
                            "lang":      _r.get("language", ""),
                            "set_code":  _r.get("set_code", ""),
                            "cn":        _r.get("cn", ""),
                            "condition": _r.get("condition", ""),
                        }
                        if _use_inv_qty and _has_inv_qty:
                            _n = max(1, int(pd.to_numeric(_r.get("qty", 1), errors="coerce") or 1))
                        else:
                            _n = _lbl_qty or 1
                        for _ in range(_n):
                            _labels_manual.append(dict(_entry))
                    _labels_manual.sort(key=lambda e: _label_sort_key(e, _release_dates))
                    _pdf_manual = _generate_label_pdf(_labels_manual)
                    st.download_button(
                        "⬇️  Descargar PDF",
                        data=_pdf_manual,
                        file_name=f"etiquetas_manual_{TODAY}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                        key="lbl_dl_manual",
                    )
