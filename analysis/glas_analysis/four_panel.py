"""Compose the four panel PDFs in a paper figure at their native sizes."""
from __future__ import annotations

import hashlib
from contextlib import ExitStack
from pathlib import Path

import pymupdf
from matplotlib.font_manager import FontProperties, findfont

PANEL_TITLE_SIZE = 22
PANEL_GAP = 9      # panels sit close enough to read as one block
PAGE_MARGIN = 4
PNG_DPI = 600
FONT_RESOURCE = "CaptionTimes"


def ink_clip(page, pad: float = 1.5) -> pymupdf.Rect:
    """Crop rectangle that keeps every mark on the page but drops blank margins."""
    dpi = 150
    pix = page.get_pixmap(dpi=dpi, colorspace=pymupdf.csGRAY)
    import numpy as np
    ink = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width) < 205
    if not ink.any():
        return page.rect
    rows = np.where(ink.any(axis=1))[0]
    cols = np.where(ink.any(axis=0))[0]
    sx, sy = page.rect.width / pix.width, page.rect.height / pix.height
    return pymupdf.Rect(
        max(page.rect.x0, cols.min() * sx - pad),
        max(page.rect.y0, rows.min() * sy - pad),
        min(page.rect.x1, (cols.max() + 1) * sx + pad),
        min(page.rect.y1, (rows.max() + 1) * sy + pad),
    )

def _fit_size(font, text: str, width: float, size: float = PANEL_TITLE_SIZE,
              minimum: float = 11.0) -> float:
    """Largest font size (<= size) at which *text* fits *width* points."""
    current = size
    while current > minimum and font.text_length(text, fontsize=current) > width:
        current -= 1.0
    return current


class FourPanelBuilder:
    def __init__(self, project: Path, output_dir: Path, font_family: str):
        self.panel_gap = PANEL_GAP
        self.page_margin = PAGE_MARGIN
        self.project = project
        self.source_dir = project / "outputs"
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.font_path = findfont(FontProperties(family=font_family), fallback_to_default=False)
        self.font = pymupdf.Font(fontfile=self.font_path)

    def compose_row(self, name: str, panels) -> dict:
        if len(panels) != 4:
            raise ValueError(f"{name} needs four source PDFs.")

        with ExitStack() as stack:
            sources = []
            for relative_path, title in panels:
                source_path = self.source_dir / relative_path
                if not source_path.is_file():
                    raise FileNotFoundError(f"{name}: missing panel PDF {source_path}")
                doc = stack.enter_context(pymupdf.open(source_path))
                if not doc.is_pdf or len(doc) != 1:
                    raise ValueError(f"Expected a single-page PDF: {source_path}")
                sources.append((source_path, doc, title))

            clips = [ink_clip(doc[0]) for _, doc, _ in sources]
            panel_widths = [clip.width for clip in clips]
            panel_heights = [clip.height for clip in clips]
            offsets = [self.page_margin + sum(panel_widths[:i]) + i * self.panel_gap
                       for i in range(len(sources))]
            chart_bottom = self.page_margin + max(panel_heights)
            label_y = chart_bottom + PANEL_TITLE_SIZE + 4
            page_width = 2 * self.page_margin + sum(panel_widths) + 3 * self.panel_gap
            page_height = label_y + self.page_margin + 4

            combined = stack.enter_context(pymupdf.open())
            page = combined.new_page(width=page_width, height=page_height)
            page.insert_font(fontname=FONT_RESOURCE, fontfile=self.font_path)
            manifest = []
            for index, ((path, doc, title), height) in enumerate(zip(sources, panel_heights)):
                width = panel_widths[index]
                left = offsets[index]
                box = pymupdf.Rect(left, chart_bottom - height, left + width, chart_bottom)
                page.show_pdf_page(box, doc, 0, keep_proportion=True, clip=clips[index])
                letter = "abcd"[index]
                label = f"({letter}) {title}"
                size = _fit_size(self.font, label, width)
                label_width = self.font.text_length(label, fontsize=size)
                page.insert_text((left + (width - label_width) / 2, label_y), label,
                                 fontname=FONT_RESOURCE, fontsize=size)
                manifest.append({
                    "label": label,
                    "source_pdf": str(path.relative_to(self.project)),
                    "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                })

            stem = self.output_dir / name
            combined.save(stem.with_suffix(".pdf"), deflate=True, garbage=3)
            stem.with_suffix(".svg").write_text(page.get_svg_image(text_as_path=True), encoding="utf-8")
            page.get_pixmap(dpi=PNG_DPI, alpha=False).save(stem.with_suffix(".png"))

            return {"panels": manifest, "png_dpi": PNG_DPI}
