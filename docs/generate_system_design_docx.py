#!/usr/bin/env python3
"""Generates docs/AQI_System_Design_Document.docx from docs/SYSTEM_DESIGN.md.

A lightweight Markdown -> Word renderer (headings, paragraphs, bullet/numbered lists,
tables, fenced code blocks, bold/inline-code) - enough for this one document's structure.
Not a general-purpose Markdown parser.

Usage:
    pip install python-docx
    python docs/generate_system_design_docx.py
"""
import re
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

DOCS_DIR = Path(__file__).resolve().parent
SRC = DOCS_DIR / "SYSTEM_DESIGN.md"
OUT = DOCS_DIR / "AQI_System_Design_Document.docx"
ARCH_PNG = DOCS_DIR / "architecture.png"

NAVY = RGBColor(0x1B, 0x2A, 0x4A)
ACCENT = RGBColor(0x8E, 0x24, 0xAA)  # purple accent, distinct from the walkthrough doc's blue
GREY = RGBColor(0x54, 0x6E, 0x7A)
HEADER_FILL = "1B2A4A"
LIGHT_FILL = "F1E9F6"


def set_cell_shading(cell, hex_color):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = tcPr.makeelement(qn("w:shd"), {})
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def shade_paragraph(paragraph, hex_color):
    pPr = paragraph._p.get_or_add_pPr()
    shd = pPr.makeelement(qn("w:shd"), {})
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:fill"), hex_color)
    pPr.append(shd)


def build_styles(doc: Document) -> None:
    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(10.5)
    normal.paragraph_format.space_after = Pt(6)
    for i, size in [(1, 24), (2, 17), (3, 13.5), (4, 12)]:
        h = doc.styles[f"Heading {i}"]
        h.font.name = "Calibri"
        h.font.size = Pt(size)
        h.font.bold = True
        h.font.color.rgb = NAVY
        h.paragraph_format.space_before = Pt(16 if i <= 2 else 10)
        h.paragraph_format.space_after = Pt(6)


def add_rich_text(paragraph, text: str) -> None:
    """Supports **bold**, `code`, and *italic* inline markers."""
    i, n = 0, len(text)
    while i < n:
        if text[i : i + 2] == "**":
            end = text.find("**", i + 2)
            if end != -1:
                r = paragraph.add_run(text[i + 2 : end])
                r.bold = True
                i = end + 2
                continue
        if text[i] == "`":
            end = text.find("`", i + 1)
            if end != -1:
                r = paragraph.add_run(text[i + 1 : end])
                r.font.name = "Consolas"
                r.font.size = Pt(9.7)
                r.font.color.rgb = RGBColor(0xAD, 0x14, 0x57)
                i = end + 1
                continue
        if text[i] == "*" and text[i : i + 2] != "**":
            end = text.find("*", i + 1)
            if end != -1:
                r = paragraph.add_run(text[i + 1 : end])
                r.italic = True
                i = end + 1
                continue
        candidates = [p for p in (text.find("**", i), text.find("`", i), text.find("*", i)) if p != -1]
        nxt = min(candidates) if candidates else n
        if nxt == i:
            # unmatched marker at i - emit it literally and force progress
            step = 2 if text[i : i + 2] == "**" else 1
            paragraph.add_run(text[i : i + step])
            i += step
            continue
        paragraph.add_run(text[i:nxt])
        i = nxt


def add_code_block(doc: Document, lines) -> None:
    p = doc.add_paragraph()
    shade_paragraph(p, "2B2B2B")
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(10)
    p.paragraph_format.left_indent = Cm(0.3)
    for i, line in enumerate(lines):
        r = p.add_run(line if line else " ")
        r.font.name = "Consolas"
        r.font.size = Pt(9)
        r.font.color.rgb = RGBColor(0xEE, 0xEE, 0xEE)
        if i < len(lines) - 1:
            r.add_break()


def add_table(doc: Document, header_cells, rows):
    ncols = len(header_cells)
    table = doc.add_table(rows=1, cols=ncols)
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = True
    for i, h in enumerate(header_cells):
        cell = table.rows[0].cells[i]
        cell.text = ""
        p = cell.paragraphs[0]
        add_rich_text(p, h)
        for r in p.runs:
            r.bold = True
            r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
            r.font.size = Pt(9.8)
        set_cell_shading(cell, HEADER_FILL)
    for r_idx, row in enumerate(rows):
        cells = table.add_row().cells
        for i in range(ncols):
            val = row[i] if i < len(row) else ""
            cell = cells[i]
            cell.text = ""
            p = cell.paragraphs[0]
            add_rich_text(p, val)
            for r in p.runs:
                r.font.size = Pt(9.3)
            if r_idx % 2 == 1:
                set_cell_shading(cell, LIGHT_FILL)
    doc.add_paragraph()


def add_cover(doc: Document):
    for _ in range(3):
        doc.add_paragraph()
    kicker = doc.add_paragraph()
    kicker.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = kicker.add_run("SYSTEM DESIGN DOCUMENT")
    r.font.size = Pt(14)
    r.font.color.rgb = ACCENT
    r.bold = True

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = title.add_run("AQI Tracking System")
    r.font.size = Pt(34)
    r.bold = True
    r.font.color.rgb = NAVY

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = sub.add_run(
        "A streaming + batch (Lambda architecture) case study, written interview-style: "
        "requirements, capacity estimation, deep-dive design, trade-offs, failure analysis, and a Q&A bank."
    )
    r.font.size = Pt(12)
    r.italic = True
    r.font.color.rgb = GREY

    for _ in range(2):
        doc.add_paragraph()
    if ARCH_PNG.exists():
        doc.add_picture(str(ARCH_PNG), width=Cm(16.5))
        doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER


# --------------------------------------------------------------------------- markdown -> docx
HEADING_RE = re.compile(r"^(#{1,4})\s+(.*)$")
TABLE_ROW_RE = re.compile(r"^\|(.+)\|\s*$")
UL_RE = re.compile(r"^(\s*)-\s+(.*)$")
NUM_RE = re.compile(r"^(\s*)\d+\.\s+(.*)$")
FENCE_RE = re.compile(r"^```")


def parse_table_row(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def convert(doc: Document, md_text: str) -> None:
    lines = md_text.splitlines()
    i, n = 0, len(lines)
    skip_toc = False
    while i < n:
        line = lines[i]

        if line.strip() == "---":
            i += 1
            continue

        if line.startswith("*Companion documents") or line.startswith(
            "*A case-study-style"
        ):
            p = doc.add_paragraph()
            add_rich_text(p, line.strip("*"))
            for r in p.runs:
                r.italic = True
                r.font.color.rgb = GREY
                r.font.size = Pt(9.5)
            i += 1
            continue

        m = HEADING_RE.match(line)
        if m:
            level, text = len(m.group(1)), m.group(2).strip()
            if text.lower() == "table of contents":
                doc.add_heading("Contents", level=1)
                skip_toc = True
                i += 1
                continue
            skip_toc = False
            level = min(level, 4)
            h = doc.add_heading("", level=level)
            add_rich_text(h, text)
            i += 1
            continue

        if skip_toc:
            i += 1
            continue

        if FENCE_RE.match(line):
            i += 1
            code_lines = []
            while i < n and not FENCE_RE.match(lines[i]):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing fence
            add_code_block(doc, code_lines)
            continue

        if TABLE_ROW_RE.match(line):
            header = parse_table_row(line)
            i += 1
            if i < n and re.match(r"^\|[\s:|-]+\|\s*$", lines[i]):
                i += 1
            rows = []
            while i < n and TABLE_ROW_RE.match(lines[i]):
                rows.append(parse_table_row(lines[i]))
                i += 1
            add_table(doc, header, rows)
            continue

        m = UL_RE.match(line)
        if m and not NUM_RE.match(line):
            indent = len(m.group(1))
            style = "List Bullet 2" if indent >= 3 else "List Bullet"
            p = doc.add_paragraph(style=style)
            add_rich_text(p, m.group(2))
            i += 1
            continue

        m = NUM_RE.match(line)
        if m:
            p = doc.add_paragraph(style="List Number")
            add_rich_text(p, m.group(2))
            i += 1
            continue

        if line.strip() == "":
            i += 1
            continue

        # plain paragraph (collect until blank line / next block)
        p = doc.add_paragraph()
        add_rich_text(p, line.strip())
        i += 1


def main():
    doc = Document()
    build_styles(doc)
    add_cover(doc)
    doc.add_page_break()

    md_text = SRC.read_text(encoding="utf-8")
    # Drop the H1 title (already on the cover) - start from the ToC heading.
    md_text = md_text.split("## Table of Contents", 1)[-1]
    md_text = "## Table of Contents" + md_text
    convert(doc, md_text)

    footer = doc.add_paragraph()
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = footer.add_run(
        "Generated from docs/SYSTEM_DESIGN.md - regenerate with python docs/generate_system_design_docx.py"
    )
    r.italic = True
    r.font.size = Pt(9)
    r.font.color.rgb = GREY

    doc.save(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
