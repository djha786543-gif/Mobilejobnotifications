# ============================================================
#  DJ's Audit Hunt — One-Shot Setup Script
#  Run this in PowerShell from wherever you want the project folder
#  Prerequisites: Git installed, GitHub account ready
# ============================================================

# ---------- 1. CREATE PROJECT FOLDER ----------
$projectName = "audit-hunt"
New-Item -ItemType Directory -Path $projectName -Force | Out-Null
Set-Location $projectName
Write-Host "Created project folder: $projectName" -ForegroundColor Green


# ---------- 2. CREATE FOLDER STRUCTURE ----------
New-Item -ItemType Directory -Path ".github\workflows" -Force | Out-Null
Write-Host "Created .github/workflows folder" -ForegroundColor Green


# ---------- 3. WRITE .gitignore ----------
@"
.env
__pycache__/
*.pyc
*.pyo
.DS_Store
venv/
.venv/
"@ | Set-Content ".gitignore"
Write-Host "Created .gitignore" -ForegroundColor Green


# ---------- 4. WRITE requirements.txt ----------
@"
streamlit>=1.35.0
pandas>=2.0.0
jobspy>=1.1.0
google-genai>=0.8.0
requests>=2.31.0
"@ | Set-Content "requirements.txt"
Write-Host "Created requirements.txt" -ForegroundColor Green


# ---------- 5. WRITE GitHub Actions workflow ----------
@"
name: Audit Hunt Scanner

on:
  schedule:
    - cron: '0 */6 * * *'   # every 6 hours
  workflow_dispatch:          # lets you trigger manually from GitHub UI

jobs:
  scan:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install jobspy google-genai pandas requests

      - name: Run scanner
        env:
          GEMINI_API_KEY: `${{ secrets.GEMINI_API_KEY }}
          NTFY_TOPIC: `${{ secrets.NTFY_TOPIC }}
        run: python master_hunter.py

      - name: Commit updated leads CSV
        run: |
          git config user.name 'github-actions[bot]'
          git config user.email 'github-actions[bot]@users.noreply.github.com'
          git add Scored_Audit_Leads.csv
          git diff --cached --quiet || git commit -m 'chore: update leads [skip ci]'
          git push
"@ | Set-Content ".github\workflows\scanner.yml"
Write-Host "Created GitHub Actions workflow" -ForegroundColor Green


# ---------- 6. CREATE placeholder CSV so Streamlit doesn't error on first load ----------
@"
Score,Title,Company,Location,Type,Link,Posted
"@ | Set-Content "Scored_Audit_Leads.csv"
Write-Host "Created empty Scored_Audit_Leads.csv" -ForegroundColor Green


# ---------- 7. INIT GIT ----------
git init
git add .
git commit -m "initial commit: audit hunt portal"
Write-Host "Git repo initialized and first commit done" -ForegroundColor Green


# ---------- NEXT STEPS (manual — can't automate GitHub auth from script) ----------
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Setup complete. Now do these 3 manual steps:" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host " STEP 1 — Create GitHub repo:" -ForegroundColor Yellow
Write-Host "   Go to https://github.com/new"
Write-Host "   Name it: audit-hunt"
Write-Host "   Set to PUBLIC (required for Streamlit Cloud free tier)"
Write-Host "   Do NOT add README/gitignore (repo must be empty)"
Write-Host ""
Write-Host " STEP 2 — Push your code (paste these after creating the repo):" -ForegroundColor Yellow
Write-Host "   git remote add origin https://github.com/YOUR_USERNAME/audit-hunt.git"
Write-Host "   git branch -M main"
Write-Host "   git push -u origin main"
Write-Host ""
Write-Host " STEP 3 — Add secrets in GitHub:" -ForegroundColor Yellow
Write-Host "   Go to: https://github.com/YOUR_USERNAME/audit-hunt/settings/secrets/actions"
Write-Host "   Add secret: GEMINI_API_KEY  → your Gemini API key"
Write-Host "   Add secret: NTFY_TOPIC      → your ntfy topic name (e.g. dj-audit-xk29q)"
Write-Host ""
Write-Host " STEP 4 — Deploy on Streamlit Cloud:" -ForegroundColor Yellow
Write-Host "   Go to: https://share.streamlit.io"
Write-Host "   Sign in with GitHub"
Write-Host "   Click 'New app' → select audit-hunt repo → main branch → app.py"
Write-Host "   Under Advanced Settings > Secrets, add:"
Write-Host "     GEMINI_API_KEY = your key"
Write-Host "     NTFY_TOPIC     = your topic name"
Write-Host "   Click Deploy"
Write-Host ""
Write-Host " STEP 5 — Set up ntfy on your phone:" -ForegroundColor Yellow
Write-Host "   Install 'ntfy' app (iOS / Android) — free"
Write-Host "   Subscribe to your NTFY_TOPIC name"
Write-Host "   You will get push alerts for every 85%+ match found"
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Done! Scanner runs every 6h automatically." -ForegroundColor Green
Write-Host " Portal URL: https://YOUR_APP.streamlit.app" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
