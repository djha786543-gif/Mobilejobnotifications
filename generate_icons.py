"""
Generate PWA app icons as solid-colour PNGs.
Runs during Render build — no Pillow dependency needed.
"""
import os
import struct
import zlib

ICON_COLOR = (59, 130, 246)   # #3B82F6 — matches theme primaryColor


def make_png(size: int, color: tuple = ICON_COLOR) -> bytes:
    r, g, b = color

    def chunk(tag: bytes, data: bytes) -> bytes:
        body = tag + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    # Each scanline: filter byte (0) + RGB pixels
    row = bytes([0]) + bytes([r, g, b] * size)
    raw = row * size
    idat = zlib.compress(raw, 9)

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", idat)
        + chunk(b"IEND", b"")
    )


if __name__ == "__main__":
    os.makedirs("static", exist_ok=True)
    for size in (192, 512):
        path = f"static/icon-{size}.png"
        with open(path, "wb") as f:
            f.write(make_png(size))
        print(f"  created {path}")
    print("Icons done.")
