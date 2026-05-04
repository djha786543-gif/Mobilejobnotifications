"""
PWA injection for Streamlit 1.55.

Approach:
- staticFolder is NOT a valid option in Streamlit 1.55, so we cannot serve
  files at custom paths. All PWA assets are embedded inline.
- Icon: generated as a minimal PNG in pure Python, encoded as data URI.
- Manifest: built as JSON and embedded as a data URI <link>.
- Uses st.components.v1.html() so the <script> actually executes
  (st.markdown strips scripts via React's dangerouslySetInnerHTML).
- Targets window.parent.document to write into the real page <head>
  from inside the component iframe.

iOS: apple-mobile-web-app-capable + apple-touch-icon → full standalone mode.
Android: data URI manifest → Chrome uses name/icon/display when user taps
         "Add to Home Screen" from the browser menu.
"""
import base64
import json
import struct
import zlib
import streamlit.components.v1 as components


def _make_png(size: int, r: int = 59, g: int = 130, b: int = 246) -> bytes:
    """Create a minimal solid-colour square PNG (pure stdlib, no Pillow)."""
    def chunk(tag: bytes, data: bytes) -> bytes:
        body = tag + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    row  = bytes([0]) + bytes([r, g, b] * size)
    idat = zlib.compress(row * size, 9)

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", idat)
        + chunk(b"IEND", b"")
    )


def _icon_uri(size: int = 192) -> str:
    png = _make_png(size)
    return "data:image/png;base64," + base64.b64encode(png).decode()


def inject_pwa(app_name: str, theme_color: str = "#3B82F6"):
    icon_192 = _icon_uri(192)
    icon_512 = _icon_uri(512)

    manifest = {
        "name":             app_name,
        "short_name":       "Job Hunt",
        "description":      "IT Audit and Biotech job tracking portal",
        "start_url":        "/",
        "scope":            "/",
        "display":          "standalone",
        "orientation":      "portrait-primary",
        "background_color": "#F8FAFC",
        "theme_color":      theme_color,
        "icons": [
            {"src": icon_192, "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": icon_512, "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ],
    }
    manifest_b64 = base64.b64encode(json.dumps(manifest).encode()).decode()
    manifest_uri = f"data:application/json;base64,{manifest_b64}"

    components.html(
        f"""<!DOCTYPE html><html><body><script>
(function () {{
  var doc  = window.parent.document;
  var head = doc.head;

  // Manifest (data URI — Chrome uses it for Add to Home Screen standalone mode)
  if (!doc.querySelector('link[rel="manifest"]')) {{
    var ml = doc.createElement('link');
    ml.rel  = 'manifest';
    ml.href = '{manifest_uri}';
    head.appendChild(ml);
  }}

  // Theme colour
  if (!doc.querySelector('meta[name="theme-color"]')) {{
    var tc = doc.createElement('meta');
    tc.name    = 'theme-color';
    tc.content = '{theme_color}';
    head.appendChild(tc);
  }}

  // iOS / Safari standalone meta tags
  [
    ['apple-mobile-web-app-capable',          'yes'],
    ['apple-mobile-web-app-status-bar-style', 'default'],
    ['apple-mobile-web-app-title',            '{app_name}'],
    ['mobile-web-app-capable',                'yes'],
  ].forEach(function(p) {{
    if (!doc.querySelector('meta[name="' + p[0] + '"]')) {{
      var m = doc.createElement('meta');
      m.name = p[0]; m.content = p[1];
      head.appendChild(m);
    }}
  }});

  // Apple touch icon (PNG data URI — works on iOS 9+)
  if (!doc.querySelector('link[rel="apple-touch-icon"]')) {{
    var al = doc.createElement('link');
    al.rel  = 'apple-touch-icon';
    al.href = '{icon_192}';
    head.appendChild(al);
  }}
}})();
</script></body></html>""",
        height=0,
    )
