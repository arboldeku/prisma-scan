# CLAUDE.md — Prisma Scan

## Identidad del proyecto

**Prisma Scan** es un sistema standalone de registro de ventas físicas para una tienda
de cartas Pokémon llamada Prisma. Funciona en tablet/móvil con escáner Bluetooth.
Genera CSVs diarios que luego se suben manualmente a Drive.

Este proyecto es **propiedad intelectual independiente**. NO es parte de Prisma.

## Regla de aislamiento (NO NEGOCIABLE)

- Este repo NO importa, referencia ni depende de código, librerías, schemas, configs
  ni repositorios de Prisma.
- PROHIBIDO usar en el código: `dim_product`, `dim_variant`, `fact_purchases`,
  `fact_sales`, `bronze_`, `silver_`, `gold_`, ni ningún nombre de tabla/función
  del pipeline de Prisma.
- PROHIBIDO escribir valores como `mixed_powertools`, `UNKNOWN`, `None`, `NaN`
  en campos de business_rarity.
- El ÚNICO vínculo con Prisma es el campo `internal_sku` compartido en el catálogo
  y en los CSV de salida. Nada más.

## Stack

- Python 3.11+
- Streamlit (UI)
- Pandas (datos)
- CSV como almacenamiento (no DB)
- Sin APIs externas, sin autenticación, sin base de datos

## Estructura del proyecto

```
prisma-scan/
├── CLAUDE.md                         ← este archivo
├── app.py                            ← app principal Streamlit
├── requirements.txt                  ← streamlit + pandas
├── .streamlit/config.toml            ← tema oscuro, config servidor
├── .claude/skills/                   ← skills para Claude Code
│   ├── scan-and-lookup.md
│   ├── sale-csv-contract.md
│   ├── void-and-returns.md
│   ├── catalog-management.md
│   └── ui-styling.md
├── data/
│   └── store_hits_catalog.csv        ← catálogo operativo (fuente de verdad SKU)
├── sales_output/                     ← CSV diarios generados por la app
├── README.md
└── .gitignore
```

## Reglas de negocio

1. **Rareza de negocio** → viene SOLO de `store_hits_catalog.csv`, nunca de fuente externa.
2. **qty = 1 siempre** → una fila = una carta física vendida.
3. **FIFO, COGS, márgenes** → NO se calculan aquí. Eso es downstream en Prisma.
4. **Anulaciones** → filas nuevas con `status=void`, nunca borrar/editar filas existentes.
5. **CSV append-only** → nunca reescribir el archivo completo, solo agregar filas.

## Flujo de datos

```
[Escáner BT] → [text_input en Streamlit] → [lookup en store_hits_catalog.csv]
→ [confirmar venta] → [append a sales_physical_scan_YYYY-MM-DD.csv]
→ [exportar al final del día] → [subir a Drive manualmente]
→ [Pipeline Bronze de Prisma lo consume]
```

## Convenciones de código

- Comentarios en español
- Funciones con docstrings descriptivos
- Variables en snake_case
- Constantes en UPPER_SNAKE_CASE
- CSS inyectado con st.markdown(unsafe_allow_html=True)
- Session state para control de flujo UI

## Qué NO hacer nunca

- Agregar autenticación o login
- Conectar a Supabase, PostgreSQL o cualquier DB
- Importar desde otros proyectos
- Crear endpoints API
- Instalar dependencias fuera de streamlit/pandas
- Modificar filas existentes en CSV (solo append)
- Asumir rarezas que no estén en el catálogo
