# ⚡ Prisma Scan — Punto de Venta Físico

Sistema standalone de registro de ventas para **Prisma**, tienda física de cartas Pokémon.
Funciona en tablet o PC con un escáner Bluetooth como periférico HID (teclado).

> **Este repositorio es propiedad intelectual independiente.** No es parte del pipeline de datos de Prisma ni depende de ningún sistema interno de Prisma. El único vínculo con Prisma es el campo `internal_sku` compartido en el catálogo y en los CSV de salida.

---

## Estructura de archivos

```
prisma-scan/
├── app.py                            ← App principal Streamlit
├── requirements.txt                  ← streamlit + pandas
├── .streamlit/
│   └── config.toml                   ← Tema oscuro, config servidor
├── data/
│   └── store_hits_catalog.csv        ← Catálogo operativo (fuente de verdad SKU)
├── sales_output/                     ← CSV diarios generados por la app
│   └── .gitkeep
├── README.md
└── .gitignore
```

---

## Instalación rápida

```bash
# 1. Clonar el repositorio
git clone <url-del-repo> prisma-scan
cd prisma-scan

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Iniciar la app
streamlit run app.py
```

La app estará disponible en `http://localhost:8501`.

---

## Configuración del escáner Bluetooth

El escáner BT debe estar configurado en **modo HID (Human Interface Device)**, es decir, que se empareje como un teclado Bluetooth estándar con la tablet o PC.

**Pasos:**
1. Emparejar el escáner con la tablet via Bluetooth (igual que un teclado).
2. Asegurarse de que el escáner tiene configurado el **sufijo Enter (CR/LF)** tras cada lectura. Esto hace que Streamlit procese el input automáticamente al escanear.
3. Con la app abierta, hacer clic en el campo "Escanea o escribe el SKU" para que tenga el foco.
4. Escanear la etiqueta de la carta — el código llega al campo y se procesa solo.

No se necesitan librerías especiales, drivers ni acceso a Bluetooth por código. El sistema operativo gestiona la conexión HID.

---

## Flujo de uso diario

```
1. Abrir la app → streamlit run app.py
2. Escanear la etiqueta de la carta con el escáner BT
3. Revisar la card del producto (nombre, rareza, idioma)
4. Ajustar el precio si es necesario (default del catálogo)
5. Pulsar ✅ CONFIRMAR VENTA
6. Repetir para cada carta vendida
7. Al final del día: pulsar 📥 Descargar CSV del día
8. Subir el CSV a Google Drive manualmente
9. El pipeline Bronze de Prisma lo detecta y lo ingesta
```

---

## Formato del CSV de ventas

El CSV se genera en `sales_output/sales_physical_scan_YYYY-MM-DD.csv`.

| # | Columna | Tipo | Descripción | Ejemplo |
|---|---------|------|-------------|---------|
| 1 | `sale_event_id` | str | UUID4 único por fila | `a1b2c3d4-e5f6-...` |
| 2 | `sale_ts` | str | ISO 8601 con segundos | `2026-03-17T14:23:05` |
| 3 | `internal_sku` | str | SKU de la tienda | `PAL-245-ESP` |
| 4 | `display_name` | str | Nombre de la carta | `Charizard ex` |
| 5 | `language` | str | Idioma (ESP/ENG/JPN) | `ESP` |
| 6 | `business_rarity` | str | Rareza de negocio | `Ultra Rare` |
| 7 | `qty` | int | Siempre 1 | `1` |
| 8 | `unit_price` | float | Precio real de venta en € | `45.00` |
| 9 | `gross_amount` | float | unit_price × qty | `45.00` |
| 10 | `channel` | str | Siempre `physical_store` | `physical_store` |
| 11 | `source_system` | str | Siempre `store_scan` | `store_scan` |
| 12 | `status` | str | `completed` / `void` | `completed` |

**Rarezas de negocio válidas:** Double Rare, Ultra Rare, Illustration Rare, Special Art Rare, Hyper Rare, Art Rare.

---

## Cómo editar el catálogo operativo

El catálogo en `data/store_hits_catalog.csv` se edita manualmente con cualquier editor de texto o Excel.

**Columnas:**
```
internal_sku,display_name,language,business_rarity,default_price
PAL-245-ESP,Charizard ex,ESP,Ultra Rare,45.00
```

**Para agregar una carta nueva:**
1. Abrir `data/store_hits_catalog.csv`
2. Añadir una fila al final con el formato: `{SET}-{NUM}-{LANG},Nombre,IDIOMA,Rareza,precio`
3. Guardar y reiniciar la app (el catálogo se cachea en sesión)

**Para actualizar precios:** editar `default_price` en el CSV y reiniciar la app. El precio es solo el default — el vendedor puede cambiarlo en cada venta.

---

## Edge cases cubiertos

| Caso | Comportamiento |
|------|---------------|
| SKU no existe en catálogo | Alerta roja, venta bloqueada |
| Doble escaneo (SKU ya vendido hoy) | Alerta amarilla + botón "Sí, es otra unidad" |
| Entrada manual de SKU | `text_input` acepta escritura directa, normaliza a mayúsculas |
| Anulación / void | Botón "↩️ Deshacer última venta" → fila con `status=void` |
| Catálogo no encontrado | `st.error` + `st.stop()` al arrancar |
| Precio editado a 0 o negativo | `min_value=0.01` en `number_input` |
| Día sin ventas | Mensaje "Sin ventas registradas hoy", no crash |
| CSV diario no existe aún | Se crea con headers en la primera venta |

---

## Nota de propiedad intelectual

Este sistema es **standalone e independiente**. No importa, referencia ni depende de:
- Código, librerías o configs del pipeline de Prisma
- Supabase, PostgreSQL ni ninguna base de datos
- APIs externas (PokémonTCG API, etc.)
- Schemas internos (`dim_product`, `fact_sales`, etc.)

El **único vínculo con el pipeline de Prisma** es:
1. El campo `internal_sku` del catálogo (sincronizado manualmente)
2. Los CSV diarios que esta app genera y que Prisma consume en Bronze

La app no sabe nada del pipeline downstream. Solo genera el CSV.

---

*Prisma Scan MVP · Standalone · v1.0*
