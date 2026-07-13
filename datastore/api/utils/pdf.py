"""
datastore/api/utils/pdf.py

F4 / FO6 — shared reportlab helper for the two server-side PDF export
endpoints (Thesis Builder, Investigation Report). Pure-Python PDF
generation (no headless browser / JS dependency), per the user's library
decision. Kept as one small shared module rather than duplicated
boilerplate in fundamentals.py and forensic.py.
"""

import io
from typing import List, Optional, Tuple

from fastapi.responses import Response
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

_STYLES = getSampleStyleSheet()
_TITLE = ParagraphStyle("AL-Title", parent=_STYLES["Title"], fontSize=18, spaceAfter=6)
_H2 = ParagraphStyle("AL-H2", parent=_STYLES["Heading2"], spaceBefore=12, spaceAfter=6)
_BODY = ParagraphStyle("AL-Body", parent=_STYLES["BodyText"], fontSize=10, leading=14)
_SUB = ParagraphStyle("AL-Sub", parent=_STYLES["Normal"], fontSize=9, textColor=colors.grey)


def build_pdf_response(
    filename: str,
    title: str,
    subtitle: str,
    sections: List[Tuple[str, List[str]]],
    table: Optional[Tuple[List[str], List[List[str]]]] = None,
) -> Response:
    """
    Render a simple titled report PDF: a title/subtitle header, then a
    sequence of (heading, bullet-lines) sections, optionally followed by a
    data table. Used identically by the Thesis Builder PDF export (F4) and
    the Investigation Report PDF export (FO6) so both stay visually
    consistent and neither hand-rolls reportlab layout twice.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm, topMargin=2 * cm, bottomMargin=2 * cm,
        title=title,
    )
    story = [Paragraph(title, _TITLE), Paragraph(subtitle, _SUB), Spacer(1, 10)]

    for heading, lines in sections:
        story.append(Paragraph(heading, _H2))
        if lines:
            for line in lines:
                story.append(Paragraph(f"&bull;&nbsp; {line}", _BODY))
        else:
            story.append(Paragraph("<i>None.</i>", _BODY))

    if table is not None:
        header, rows = table
        data = [header] + rows
        t = Table(data, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2b2f38")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f4f4f4")]),
        ]))
        story.append(Spacer(1, 10))
        story.append(t)

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
