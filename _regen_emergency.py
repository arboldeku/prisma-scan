"""
_regen_emergency.py — Regenera los 3 PDFs de emergencia con internal_sku en el barcode.
Uso: python _regen_emergency.py
"""
import csv
from datetime import date
from pathlib import Path

from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.colors import black, white
from reportlab.graphics.barcode import code128
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

pdfmetrics.registerFont(TTFont("GothicBold", "C:/Windows/Fonts/GOTHICB.TTF"))

PAGE_W = 60 * mm
PAGE_H = 30 * mm
TOP_H  = 13 * mm
LEFT_W = 18 * mm
BC_H   = 15 * mm
PAD    =  1 * mm

LANG_MAP = {
    "English": "ENG", "Spanish": "ESP", "Korean": "KOR",
    "Japanese": "JPN", "French": "FRA", "German": "DEU",
    "Italian": "ITA", "Portuguese": "POR",
}

EMERGENCY_DIR = Path("labels_output/Labels Emergency")
CATALOG_PATH  = Path("data/hits_catalog.csv")


def load_catalog():
    catalog = {}
    with open(CATALOG_PATH, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            catalog[(row["cardmarket_id"], row["lang"])] = row["internal_sku"]
    return catalog


def draw_label(c: canvas.Canvas, data: dict):
    W, H = PAGE_W, PAGE_H

    c.setFillColor(white)
    c.setStrokeColor(black)
    c.setLineWidth(0.8)
    c.rect(0, 0, W, H, fill=1, stroke=1)

    top_y = H - TOP_H
    c.setFillColor(black)
    c.rect(0, top_y, LEFT_W, TOP_H, fill=1, stroke=0)

    blk_pad = 1.5 * mm
    blk_w   = LEFT_W - 2 * blk_pad

    prisma_fs = 24.0
    while stringWidth("PRISMA", "GothicBold", prisma_fs) > blk_w and prisma_fs > 6:
        prisma_fs -= 0.5
    c.setFillColor(white)
    c.setFont("GothicBold", prisma_fs)
    pw = stringWidth("PRISMA", "GothicBold", prisma_fs)
    c.drawString((LEFT_W - pw) / 2, top_y + TOP_H * 0.50, "PRISMA")

    sub_fs = prisma_fs * 0.48
    while stringWidth("COLLECT & PLAY!", "GothicBold", sub_fs) > blk_w and sub_fs > 2:
        sub_fs -= 0.25
    c.setFont("GothicBold", sub_fs)
    sw = stringWidth("COLLECT & PLAY!", "GothicBold", sub_fs)
    sx = (LEFT_W - sw) / 2
    sy = top_y + TOP_H * 0.24
    for dx, dy in [(0, 0), (0.3, 0), (0, 0.3), (0.3, 0.3)]:
        c.drawString(sx + dx, sy + dy, "COLLECT & PLAY!")

    c.setStrokeColor(black)
    c.setLineWidth(0.6)
    c.line(LEFT_W, top_y, LEFT_W, H)

    right_x = LEFT_W + 2 * PAD
    right_w  = W - LEFT_W - 2 * PAD

    c.setFillColor(black)

    name_str = f"{data['name']} ({data['lang']})"
    fs1 = 9.0
    while stringWidth(name_str, "Helvetica-BoldOblique", fs1) > right_w and fs1 > 5:
        fs1 -= 0.5
    c.setFont("Helvetica-BoldOblique", fs1)
    c.drawString(right_x, top_y + TOP_H * 0.65, name_str)

    c.setFont("Helvetica-Bold", 8)
    c.drawString(right_x, top_y + TOP_H * 0.38, f"{data['set']} - {data['cn']}")
    c.drawString(right_x, top_y + TOP_H * 0.10, data["sku"])

    c.setStrokeColor(black)
    c.setLineWidth(0.6)
    c.line(0, top_y, W, top_y)

    bc_probe = code128.Code128(
        data["sku"], barHeight=1, barWidth=1,
        humanReadable=False, lquiet=0, rquiet=0,
    )
    bc_margin = 2 * mm
    bar_w = (W - 2 * bc_margin) / bc_probe.width
    bc_obj = code128.Code128(
        data["sku"],
        barHeight=BC_H - 1 * mm,
        barWidth=bar_w,
        humanReadable=False,
        barFillColor=black,
        lquiet=0, rquiet=0,
    )
    bc_y = (top_y - BC_H) / 2

    c.saveState()
    p = c.beginPath()
    p.rect(0, 0, W, top_y)
    c.clipPath(p, stroke=0)
    bc_obj.drawOn(c, bc_margin, bc_y)
    c.restoreState()


def _cn_sort_key(entry: dict) -> tuple:
    """Sort key: (cardmarket_id numeric for set order, cn numeric, lang)."""
    sku = entry["sku"]
    # Extract cardmarket_id from SKU prefix (e.g. "733601-0002" → 733601)
    try:
        cm_id = int(sku.split("-")[0])
    except (ValueError, IndexError):
        try:
            cm_id = int(sku)
        except ValueError:
            cm_id = 999999
    # Extract numeric part of cn (e.g. "065/165" → 65, "TG01" → 1)
    import re as _re
    m = _re.search(r"(\d+)", entry["cn"])
    cn_num = int(m.group(1)) if m else 9999
    return (cm_id, cn_num, entry["lang"])


def build_labels_from_csv(csv_path: Path, catalog: dict) -> list:
    rows_list = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            lang = LANG_MAP.get(row["language"], row["language"])
            qty  = int(row["quantity"])
            key  = (row["cardmarketId"], lang)
            sku  = catalog.get(key, row["cardmarketId"])
            rows_list.append({
                "sku":  sku,
                "name": row["name"],
                "lang": lang,
                "set":  row["setCode"].upper(),
                "cn":   row["cn"],
                "_qty": qty,
            })

    # Sort by set (approx chronological via cardmarket_id) → cn numeric → lang
    rows_list.sort(key=_cn_sort_key)

    labels = []
    for entry in rows_list:
        qty = entry.pop("_qty")
        for _ in range(qty):
            labels.append(dict(entry))
    return labels


def main():
    catalog = load_catalog()
    csv_files = ["Evolutions + Galleries", "Galleries", "Promos Varias + Glaceon"]

    for name in csv_files:
        csv_path = EMERGENCY_DIR / f"{name}.csv"
        pdf_path = EMERGENCY_DIR / f"{name}.pdf"

        labels = build_labels_from_csv(csv_path, catalog)
        total  = len(labels)

        c = canvas.Canvas(str(pdf_path), pagesize=(PAGE_W, PAGE_H))
        c.setTitle(f"Prisma — {name}")

        for i, data in enumerate(labels):
            draw_label(c, data)
            if i < total - 1:
                c.showPage()

        c.save()
        print(f"  {name:<35} {total:3} etiquetas -> {pdf_path.name}")

    print("\nListo.")


if __name__ == "__main__":
    main()
