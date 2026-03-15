name: Daily MUR SST Retrieval

on:
  schedule:
    - cron: "0 0 * * *"   # 00:00 UTC every day
  workflow_dispatch:        # allow manual trigger from the Actions tab

permissions:
  contents: write           # needed to push the downloaded files back to the repo

jobs:
  retrieve-sst:
    name: Fetch MUR SST and commit
    runs-on: ubuntu-latest
    timeout-minutes: 120    # large regional downloads can take a while

    steps:
      # ── 1. Checkout ──────────────────────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 1

      # ── 2. Python setup ──────────────────────────────────────────────────────
      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      # ── 3. Install dependencies ───────────────────────────────────────────────
      - name: Install Python dependencies
        run: python -m pip install -r requirements.txt

      # ── 4. Run retrieval script ───────────────────────────────────────────────
      - name: Run DailySSTRetrieval.py
        run: python DailySSTRetrieval.py

      # ── 5. Commit and push results ────────────────────────────────────────────
      - name: Commit DailySST data to repository
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add DailySST/

          # Only commit if something actually changed
          if git diff --cached --quiet; then
            echo "No changes to commit — files are already up to date."
          else
            git commit -m "chore: daily MUR SST update $(date -u '+%Y-%m-%d')"
            git push
          fi
