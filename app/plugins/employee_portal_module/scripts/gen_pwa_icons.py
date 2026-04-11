"""Write solid-color PWA PNGs (stdlib only). Run: python scripts/gen_pwa_icons.py"""
import struct
import zlib
from pathlib import Path


def _chunk(tag: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)


def write_png(path: Path, w: int, h: int, rgba=(13, 110, 253, 255)) -> None:
    row = b"\x00" + bytes(rgba) * w
    raw = row * h
    comp = zlib.compress(raw, 9)
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)
    png = b"\x89PNG\r\n\x1a\n" + _chunk(b"IHDR", ihdr) + _chunk(b"IDAT", comp) + _chunk(b"IEND", b"")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(png)


if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent / "static" / "pwa"
    write_png(base / "icon-192.png", 192, 192)
    write_png(base / "icon-512.png", 512, 512)
    print("Wrote", base)
