from __future__ import annotations

import json
import textwrap
import urllib.request
from pathlib import Path
from typing import Any, Dict, List


def profile_reel_posts(profile_item: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
    reels: List[Dict[str, Any]] = []
    for post in profile_item.get("latestPosts") or []:
        if post.get("type") == "Video" and post.get("videoUrl"):
            reels.append(
                {
                    "short_code": post.get("shortCode") or "",
                    "instagram_url": post.get("url") or "",
                    "caption": post.get("caption") or "",
                    "video_url": post.get("videoUrl") or "",
                    "timestamp": post.get("timestamp"),
                }
            )
        for child in post.get("childPosts") or []:
            if child.get("type") == "Video" and child.get("videoUrl"):
                reels.append(
                    {
                        "short_code": child.get("shortCode") or post.get("shortCode") or "",
                        "instagram_url": child.get("url") or post.get("url") or "",
                        "caption": child.get("caption") or post.get("caption") or "",
                        "video_url": child.get("videoUrl") or "",
                        "timestamp": child.get("timestamp") or post.get("timestamp"),
                    }
                )

    unique: List[Dict[str, Any]] = []
    seen = set()
    for reel in reels:
        key = reel["video_url"] or reel["instagram_url"] or reel["short_code"]
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(reel)
        if len(unique) >= limit:
            break
    return unique


def download_media(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=180) as response:
        return response.read()


def transcript_output_paths(base_dir: Path, handle: str) -> Dict[str, Path]:
    transcript_dir = base_dir / "transcripts"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    return {
        "json": transcript_dir / f"{handle}_reels_transcripts.json",
        "pdf": transcript_dir / f"{handle}_reels_transcripts.pdf",
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def write_simple_pdf(path: Path, title: str, transcript_rows: List[Dict[str, Any]]) -> None:
    lines = [title, ""]
    for idx, row in enumerate(transcript_rows, 1):
        lines.append(f"Reel {idx}: {row.get('instagram_url') or row.get('short_code')}")
        caption = (row.get("caption") or "").strip()
        if caption:
            lines.append("Caption:")
            lines.extend(textwrap.wrap(caption, width=95))
        transcript = (row.get("transcript") or "").strip()
        lines.append("Transcript:")
        lines.extend(textwrap.wrap(transcript or "[no transcript returned]", width=95))
        lines.append("")

    pages: List[List[str]] = []
    current_page: List[str] = []
    for line in lines:
        current_page.append(line)
        if len(current_page) >= 42:
            pages.append(current_page)
            current_page = []
    if current_page:
        pages.append(current_page)

    objects: List[bytes] = []

    def add_object(content: str | bytes) -> int:
        data = content.encode("utf-8") if isinstance(content, str) else content
        objects.append(data)
        return len(objects)

    font_id = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    page_ids: List[int] = []
    content_ids: List[int] = []

    for page_lines in pages:
        text_commands = ["BT", "/F1 10 Tf", "50 780 Td", "14 TL"]
        for line in page_lines:
            safe = _pdf_escape(line)
            text_commands.append(f"({safe}) Tj")
            text_commands.append("T*")
        text_commands.append("ET")
        stream = "\n".join(text_commands).encode("utf-8")
        content_id = add_object(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        )
        content_ids.append(content_id)
        page_id = add_object(
            f"<< /Type /Page /Parent PAGES_ID 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>"
        )
        page_ids.append(page_id)

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    pages_id = add_object(f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>")

    for page_id in page_ids:
        objects[page_id - 1] = objects[page_id - 1].replace(b"PAGES_ID", str(pages_id).encode("ascii"))

    catalog_id = add_object(f"<< /Type /Catalog /Pages {pages_id} 0 R >>")

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf.extend(f"{idx} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref_start = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_start}\n%%EOF\n"
        ).encode("ascii")
    )
    path.write_bytes(bytes(pdf))
