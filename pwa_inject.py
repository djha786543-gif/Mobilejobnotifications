"""
Inject PWA meta tags and service-worker registration into the Streamlit page.
Call once near the top of each page (after set_page_config).

iOS  → apple-mobile-web-app-capable + apple-touch-icon  (no SW needed)
Android → manifest.json + sw.js registered at scope '/'
"""
import streamlit as st


def inject_pwa(app_name: str, theme_color: str = "#3B82F6"):
    st.markdown(
        f"""
<script>
(function () {{
  // ── Manifest link ──────────────────────────────────────────────
  if (!document.querySelector('link[rel="manifest"]')) {{
    var ml = document.createElement('link');
    ml.rel  = 'manifest';
    ml.href = '/manifest.json';
    document.head.appendChild(ml);
  }}

  // ── Theme colour ───────────────────────────────────────────────
  if (!document.querySelector('meta[name="theme-color"]')) {{
    var tc = document.createElement('meta');
    tc.name    = 'theme-color';
    tc.content = '{theme_color}';
    document.head.appendChild(tc);
  }}

  // ── iOS / Safari meta tags ─────────────────────────────────────
  var iosMeta = [
    ['apple-mobile-web-app-capable',          'yes'],
    ['apple-mobile-web-app-status-bar-style', 'default'],
    ['apple-mobile-web-app-title',            '{app_name}'],
    ['mobile-web-app-capable',                'yes'],
  ];
  iosMeta.forEach(function(pair) {{
    if (!document.querySelector('meta[name="' + pair[0] + '"]')) {{
      var m = document.createElement('meta');
      m.name    = pair[0];
      m.content = pair[1];
      document.head.appendChild(m);
    }}
  }});

  // ── Apple touch icon ───────────────────────────────────────────
  if (!document.querySelector('link[rel="apple-touch-icon"]')) {{
    var al = document.createElement('link');
    al.rel  = 'apple-touch-icon';
    al.href = '/icon-192.png';
    document.head.appendChild(al);
  }}

  // ── Service worker (Android Chrome install prompt) ─────────────
  if ('serviceWorker' in navigator) {{
    window.addEventListener('load', function () {{
      navigator.serviceWorker
        .register('/sw.js', {{ scope: '/' }})
        .then(function (r) {{ console.log('[PWA] SW registered, scope:', r.scope); }})
        .catch(function (e) {{ console.warn('[PWA] SW registration failed:', e); }});
    }});
  }}
}})();
</script>
""",
        unsafe_allow_html=True,
    )
