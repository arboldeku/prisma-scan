"""
Prisma Scan — Punto de Venta Físico
Sistema standalone de registro de ventas para Prisma, tienda de cartas Pokémon.
Genera CSV diarios para su posterior carga manual a Drive → Pipeline Bronze.
"""

import threading
import uuid
from datetime import datetime
from pathlib import Path

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
TODAY        = datetime.now().strftime("%Y-%m-%d")
DAILY_CSV    = SALES_DIR / f"sales_physical_scan_{TODAY}.csv"

USE_SHEETS = "gcp_service_account" in st.secrets

CSV_COLUMNS = [
    "sale_event_id",
    "sale_ts",
    "internal_sku",
    "display_name",
    "language",
    "business_rarity",
    "qty",
    "unit_price",
    "gross_amount",
    "channel",
    "source_system",
    "status",
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
    df = pd.read_csv(CATALOG_PATH, dtype={"internal_sku": str})
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
    """Escribe en Sheets desde un hilo de fondo — no bloquea la UI."""
    try:
        sheet = get_sheet()
        sheet.append_row([record[col] for col in CSV_COLUMNS], value_input_option="RAW")
    except Exception:
        pass  # el dato ya está en session_state; no es crítico


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
      1. Añade a session_state.sales → UI actualizada al instante (0ms).
      2. Escribe en Sheets/CSV en hilo de fondo → no bloquea la pantalla.
    """
    st.session_state.sales.append(record)
    target = _write_to_sheets if USE_SHEETS else _write_to_csv
    threading.Thread(target=target, args=(record,), daemon=True).start()


def register_scan(sku: str) -> tuple[bool, str]:
    """
    Registra el escaneo de un SKU.
    Retorna (éxito, mensaje_feedback).
    """
    if sku not in catalog.index:
        return False, f"SKU no encontrado: {sku}"

    product = catalog.loc[sku]
    save_sale({
        "sale_event_id":  str(uuid.uuid4()),
        "sale_ts":        datetime.now().isoformat(timespec="seconds"),
        "internal_sku":   sku,
        "display_name":   product["display_name"],
        "language":       product["language"],
        "business_rarity": product["business_rarity"],
        "qty":            1,
        "unit_price":     0.0,
        "gross_amount":   0.0,
        "channel":        "physical_store",
        "source_system":  "store_scan",
        "status":         "completed",
    })
    return True, f"{product['display_name']} · {product['language']} · {product['business_rarity']}"


def void_sale(original: dict) -> None:
    """
    Anula una venta específica añadiendo fila void (nunca edita la original).
    """
    save_sale({
        "sale_event_id":  str(uuid.uuid4()),
        "sale_ts":        datetime.now().isoformat(timespec="seconds"),
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
# B2) ENTRADA MANUAL — por si falla una etiqueta
# ─────────────────────────────────────────────
with st.expander("Entrada manual (etiqueta dañada)"):
    # Opciones de set_code + nombre para el selector
    sets_in_catalog = (
        catalog.reset_index()[["set_code", "set_name"]]
        .drop_duplicates()
        .sort_values("set_name")
        if "set_name" in catalog.columns
        else catalog.reset_index()[["set_code"]].drop_duplicates().sort_values("set_code")
    )
    set_options = sets_in_catalog["set_code"].tolist() if "set_code" in sets_in_catalog.columns else []

    col_a, col_b, col_c = st.columns([1.8, 1.0, 1.0])
    with col_a:
        manual_set = st.selectbox("Expansión", options=set_options, index=0 if set_options else None, label_visibility="visible")
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
# C) FEEDBACK DEL ÚLTIMO ESCANEO
# ─────────────────────────────────────────────
if st.session_state.last_msg:
    css = "alert-ok" if st.session_state.last_ok else "alert-error"
    icon = "✅" if st.session_state.last_ok else "❌"
    st.markdown(
        f'<div class="alert {css}">{icon} {st.session_state.last_msg}</div>',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
# D) TABLA DEL DÍA — leída de session_state, sin llamadas de red
# ─────────────────────────────────────────────
df_sales = pd.DataFrame(st.session_state.sales, columns=CSV_COLUMNS) if st.session_state.sales else pd.DataFrame(columns=CSV_COLUMNS)

st.markdown('<p class="summary-title">Ventas de hoy</p>', unsafe_allow_html=True)

if df_sales.empty:
    st.markdown(
        '<div class="alert alert-warn" style="text-align:center; margin-top:0.5rem;">'
        "Sin ventas registradas hoy — esperando primer escaneo</div>",
        unsafe_allow_html=True,
    )
else:
    # Métricas netas
    n_completed = (df_sales["status"] == "completed").sum()
    n_voided    = (df_sales["status"] == "void").sum()
    net_count   = n_completed - n_voided

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            f'<div class="metric-card"><p class="metric-value">{net_count}</p>'
            '<p class="metric-label">Cartas netas</p></div>',
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f'<div class="metric-card"><p class="metric-value">{len(df_sales)}</p>'
            '<p class="metric-label">Entradas totales</p></div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Cabecera de columnas
    HDR = st.columns([0.45, 0.7, 2.8, 1.3, 0.55, 1.1, 0.45])
    for label, col in zip(["#", "Hora", "Carta", "SKU", "Lang", "Rareza", ""], HDR):
        col.markdown(
            f'<span style="color:var(--prisma-muted);font-size:0.7rem;'
            f'font-weight:700;text-transform:uppercase;">{label}</span>',
            unsafe_allow_html=True,
        )
    st.markdown(
        '<hr style="margin:0.25rem 0 0.4rem 0; border-color:var(--prisma-border);">',
        unsafe_allow_html=True,
    )

    # Filas — orden cronológico ascendente (más reciente abajo)
    row_num = 0
    for _, row in df_sales.iterrows():
        is_void = row["status"] == "void"
        if not is_void:
            row_num += 1

        try:
            time_str = datetime.fromisoformat(str(row["sale_ts"])).strftime("%H:%M")
        except (ValueError, TypeError):
            time_str = "—"

        # Estilo tachado para filas void
        dim = "color:var(--prisma-muted);text-decoration:line-through;" if is_void else ""

        cols = st.columns([0.45, 0.7, 2.8, 1.3, 0.55, 1.1, 0.45])

        cols[0].markdown(
            f'<span style="{dim}font-size:0.8rem;">{"🚫" if is_void else row_num}</span>',
            unsafe_allow_html=True,
        )
        cols[1].markdown(
            f'<span style="{dim}font-size:0.78rem;font-family:monospace;">{time_str}</span>',
            unsafe_allow_html=True,
        )
        cols[2].markdown(
            f'<span style="{dim}font-size:0.85rem;">{row["display_name"]}</span>',
            unsafe_allow_html=True,
        )
        cols[3].markdown(
            f'<span style="{dim}font-size:0.7rem;font-family:monospace;color:var(--prisma-muted);">'
            f'{row["internal_sku"]}</span>',
            unsafe_allow_html=True,
        )
        cols[4].markdown(
            f'<span style="{dim}font-size:0.75rem;">{row["language"]}</span>',
            unsafe_allow_html=True,
        )
        cols[5].markdown(
            f'<span style="{dim}font-size:0.72rem;">{row["business_rarity"]}</span>',
            unsafe_allow_html=True,
        )

        # Botón ✕ solo en filas activas (no en voids)
        if not is_void:
            if cols[6].button("✕", key=f"void_{row['sale_event_id']}"):
                void_sale(row.to_dict())
                st.session_state.last_msg = f"Anulada: {row['display_name']}"
                st.session_state.last_ok  = True
                st.rerun()

# ─────────────────────────────────────────────
# E) EXPORTAR CSV DEL DÍA
# ─────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)

if not df_sales.empty:
    st.download_button(
        label="📥 Exportar CSV del día",
        data=df_sales.to_csv(index=False).encode("utf-8"),
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
