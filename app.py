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

# Componente custom de escaneo (detecta velocidad de escáner sin depender de Enter)
_SCANNER_DIR = Path(__file__).parent / "_scanner_component"
_scanner_input = components.declare_component("scanner_input", path=str(_SCANNER_DIR))

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
CATALOG_PATH = Path("data/hits_catalog.csv")
SALES_DIR    = Path("sales_output")
TODAY        = datetime.now(TZ_MADRID).strftime("%Y-%m-%d")
DAILY_CSV    = SALES_DIR / f"sales_physical_scan_{TODAY}.csv"

USE_SHEETS = "gcp_service_account" in st.secrets

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
def load_catalog() -> pd.DataFrame:
    """Carga el catálogo operativo desde disco. Se cachea en sesión."""
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


def load_daily_sales() -> pd.DataFrame:
    """
    Lee las ventas del día actual.
    Sin cache para detectar cambios al instante.
    """
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
        for i, col in enumerate(CSV_COLUMNS):
            if col not in headers:
                sheet.update_cell(1, len(headers) + 1, col)
                headers.append(col)
        sheet.append_row(
            [record.get(col, "") for col in CSV_COLUMNS],
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


def save_sale(record: dict) -> None:
    """
    Persiste una venta en dos pasos:
      1. Añade a session_state.sales → UI actualizada al instante.
      2. Escribe en Sheets/CSV de forma síncrona → garantiza persistencia al cerrar navegador.
    """
    st.session_state.sales.append(record)
    if USE_SHEETS:
        _write_to_sheets(record)
    else:
        _write_to_csv(record)


def register_scan(sku: str) -> tuple[bool, str]:
    """
    Registra el escaneo de un SKU.
    Retorna (éxito, mensaje_feedback).
    """
    if sku not in catalog.index:
        # Fallback: buscar por cardmarket_id (etiquetas impresas sin sufijo -0001)
        if "cardmarket_id" in catalog.columns:
            matches = catalog[catalog["cardmarket_id"] == sku]
            if not matches.empty:
                sku = matches.index[0]
            else:
                return False, f"SKU no encontrado: {sku}"
        else:
            return False, f"SKU no encontrado: {sku}"

    product = catalog.loc[sku]
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
        "discount_eur":   float(st.session_state.get("discount_eur_input", 0.0)),
        "channel":        "physical_store",
        "source_system":  "store_scan",
        "status":         "completed",
        "sale_type":      scan_mode,
        "payment_method": payment_method,
        "money_direction": money_direction,
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
        "discount_eur":   original.get("discount_eur", 0.0),
        "session_id":     original.get("session_id", ""),
    })


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

# ─────────────────────────────────────────────
# CATÁLOGO
# ─────────────────────────────────────────────
catalog = load_catalog()

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

# ─────────────────────────────────────────────
# B) COMPONENTE DE ESCANEO
#
# Componente custom con JS propio. Detecta la velocidad de tecleo:
#   - Escáner: ~5-15 ms entre caracteres → dispara solo al terminar
#   - Humano: >80 ms → dispara tras 180 ms de inactividad
# No depende del Enter ni del blur del navegador.
# La key cambia con scan_counter para que el componente se reinicie
# tras cada scan exitoso (limpia el valor en session_state).
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

# ─────────────────────────────────────────────
# B3) DESCUENTO EN EUROS
# ─────────────────────────────────────────────
st.number_input(
    "Descuento (€)",
    min_value=0.0, max_value=9999.0,
    value=0.0,
    step=0.50,
    format="%.2f",
    key="discount_eur_input",
)

# ─────────────────────────────────────────────
# B4) ENTRADA MANUAL — por si falla una etiqueta
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
    cat_r = catalog.reset_index()
    # Filtrar solo cartas con stock disponible (amount > 0)
    if "amount" in cat_r.columns:
        cat_r = cat_r[pd.to_numeric(cat_r["amount"], errors="coerce").fillna(0) > 0]
    search_name = st.text_input("Nombre Pokémon", placeholder="Charizard...", key="search_name")
    s1, s2, s3 = st.columns(3)
    with s1:
        set_opts = ["Todos"] + sorted(cat_r["set_code"].dropna().unique().tolist())
        search_set = st.selectbox("Expansión", set_opts, key="search_set")
    with s2:
        lang_opts = ["Todos"] + sorted(cat_r["language"].dropna().unique().tolist())
        search_lang = st.selectbox("Idioma", lang_opts, key="search_lang")
    with s3:
        rar_opts = ["Todos"] + sorted(cat_r["business_rarity"].dropna().unique().tolist())
        search_rar = st.selectbox("Rareza", rar_opts, key="search_rar")

    has_filter = search_name or search_set != "Todos" or search_lang != "Todos" or search_rar != "Todos"
    if has_filter:
        mask = pd.Series(True, index=cat_r.index)
        if search_name:
            mask &= cat_r["display_name"].str.contains(search_name, case=False, na=False)
        if search_set != "Todos":
            mask &= cat_r["set_code"] == search_set
        if search_lang != "Todos":
            mask &= cat_r["language"] == search_lang
        if search_rar != "Todos":
            mask &= cat_r["business_rarity"] == search_rar
        res = cat_r[mask][["internal_sku", "display_name", "language", "business_rarity", "set_code", "cn"]]
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
col_tkt, col_new = st.columns([3, 1])
col_tkt.markdown('<p class="summary-title" style="margin:0.6rem 0 0.2rem 0;">🧾 Ticket actual</p>', unsafe_allow_html=True)
if col_new.button("➕ Nuevo", key="new_ticket", use_container_width=True):
    st.session_state.current_session_id = str(uuid.uuid4())[:8]
    st.rerun()

sess_id = st.session_state.current_session_id
sess_sales = [s for s in st.session_state.sales
              if s.get("session_id") == sess_id and s.get("status") == "completed"]

if not sess_sales:
    st.markdown('<span style="color:var(--prisma-muted);font-size:0.8rem;">Sin artículos en este ticket — escanea para añadir</span>', unsafe_allow_html=True)
else:
    df_sess = pd.DataFrame(sess_sales)
    grp_sess = df_sess.groupby(
        ["internal_sku", "display_name", "language", "business_rarity"],
        as_index=False
    )["qty"].sum().sort_values("display_name")
    disc = df_sess["discount_eur"].astype(float).max() if "discount_eur" in df_sess.columns else 0.0
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
    disc_line = f' · <span style="color:var(--prisma-danger);">-{disc:.2f}€ dto.</span>' if disc > 0 else ""
    st.markdown(
        f'<div style="margin-top:8px;font-size:0.78rem;color:var(--prisma-muted);text-align:right;">'
        f'{total_u} carta(s){disc_line}</div>',
        unsafe_allow_html=True,
    )

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
# E) EXPORTAR CSV DEL DÍA
# ─────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)

if not df_sales.empty:
    df_completed = df_sales[df_sales["status"] == "completed"].copy()
    df_void      = df_sales[df_sales["status"] == "void"].copy()
    grp_cols = ["internal_sku", "display_name", "language", "business_rarity",
                "unit_price", "channel", "source_system", "status",
                "sale_type", "payment_method", "money_direction", "discount_eur"]
    grp_cols = [c for c in grp_cols if c in df_completed.columns]
    if not df_completed.empty:
        df_agg = df_completed.groupby(grp_cols, as_index=False).agg({"qty": "sum", "gross_amount": "sum"})
        df_agg["sale_event_id"] = df_agg["internal_sku"].apply(lambda x: f"{x}-agg")
        df_agg["sale_ts"]       = datetime.now(TZ_MADRID).isoformat(timespec="seconds")
        df_agg["session_id"]    = "aggregated"
        df_export = pd.concat([df_agg, df_void], ignore_index=True)
    else:
        df_export = df_void
    for col in CSV_COLUMNS:
        if col not in df_export.columns:
            df_export[col] = ""
    df_export = df_export[CSV_COLUMNS]

    st.download_button(
        label="📥 Exportar CSV del día",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name=DAILY_CSV.name,
        mime="text/csv",
        use_container_width=True,
    )
    st.markdown(
        '<p style="font-size:0.72rem;color:var(--prisma-muted);text-align:center;">'
        "Descarga el CSV y súbelo a Drive para que entre al pipeline Bronze.</p>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<p style="font-size:0.78rem;color:var(--prisma-muted);text-align:center;">'
        "El CSV estará disponible cuando haya al menos una venta.</p>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
# F) FOOTER
# ─────────────────────────────────────────────
st.markdown(
    f'<div class="prisma-footer">Prisma Scan · {TODAY} · v2.0</div>',
    unsafe_allow_html=True,
)
