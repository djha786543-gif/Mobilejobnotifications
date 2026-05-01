"""
PWA injection for Streamlit apps.

- Uses st.components.v1.html() so the <script> actually EXECUTES
  (st.markdown unsafe_allow_html strips scripts via React's dangerouslySetInnerHTML).
- Targets window.parent.document so it can write into the real page <head>
  (components run in a sandboxed iframe).
- Streamlit 1.55 serves staticFolder at /app/static/<filename>.
"""
import streamlit.components.v1 as components


def inject_pwa(app_name: str, theme_color: str = "#3B82F6"):
    components.html(
        f"""
<!DOCTYPE html>
<html><body>
<script>
(function () {{
  var doc = window.parent.document;
  var head = doc.head;

  // ── Manifest link (/app/static/manifest.json) ──────────────────
  if (!doc.querySelector('link[rel="manifest"]')) {{
    var ml = doc.createElement('link');
    ml.rel  = 'manifest';
    ml.href = '/app/static/manifest.json';
    head.appendChild(ml);
  }}

  // ── Theme colour ───────────────────────────────────────────────
  if (!doc.querySelector('meta[name="theme-color"]')) {{
    var tc = doc.createElement('meta');
    tc.name    = 'theme-color';
    tc.content = '{theme_color}';
    head.appendChild(tc);
  }}

  // ── iOS / Safari standalone meta tags ─────────────────────────
  var iosMetas = [
    ['apple-mobile-web-app-capable',          'yes'],
    ['apple-mobile-web-app-status-bar-style', 'default'],
    ['apple-mobile-web-app-title',            '{app_name}'],
    ['mobile-web-app-capable',                'yes'],
  ];
  iosMetas.forEach(function(pair) {{
    if (!doc.querySelector('meta[name="' + pair[0] + '"]')) {{
      var m = doc.createElement('meta');
      m.name    = pair[0];
      m.content = pair[1];
      head.appendChild(m);
    }}
  }});

  // ── Apple touch icon ───────────────────────────────────────────
  if (!doc.querySelector('link[rel="apple-touch-icon"]')) {{
    var al = doc.createElement('link');
    al.rel  = 'apple-touch-icon';
    al.href = '/app/static/icon-192.png';
    head.appendChild(al);
  }}

  // ── Service worker ─────────────────────────────────────────────
  // sw.js lives at /app/static/sw.js (Streamlit 1.55 staticFolder path).
  // We set Service-Worker-Allowed header via the SW response header trick:
  // the SW itself sets its own scope via the registration options.
  // Without a root-level SW, Chrome won't auto-prompt install but
  // "Add to Home Screen" from browser menu opens the app in standalone mode.
  if ('serviceWorker' in window.parent.navigator) {{
    window.parent.addEventListener('load', function () {{
      window.parent.navigator.serviceWorker
        .register('/app/static/sw.js')
        .then(function (r) {{
          console.log('[PWA] SW registered:', r.scope);
        }})
        .catch(function (e) {{
          console.warn('[PWA] SW skipped (scope limit):', e.message);
        }});
    }});
  }}
}})();
</script>
</body></html>
""",
        height=0,
    )
