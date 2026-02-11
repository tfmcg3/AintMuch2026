#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="dev_journal.log"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

show_menu() {
  clear
  echo "╔═══════════════════════════════════════════╗"
  echo "║   Dutchie Menu Scraper - Control Panel    ║"
  echo "╠═══════════════════════════════════════════╣"
  echo "║                                           ║"
  echo "║  1) Run Pytest (Data Normalization)       ║"
  echo "║  2) Run Canary Test (Live API Health)     ║"
  echo "║  3) Check Environment Secrets             ║"
  echo "║  4) Run Scraper Locally                   ║"
  echo "║  5) View Dev Journal Log                  ║"
  echo "║  6) Clean Cache & Temp Files              ║"
  echo "║  0) Exit                                  ║"
  echo "║                                           ║"
  echo "╚═══════════════════════════════════════════╝"
  echo ""
  printf "  Select an option [0-6]: "
}

run_pytest() {
  log "--- PYTEST: Data Normalization Tests ---"
  echo ""
  python -m pytest tests/test_parser.py -v 2>&1 | tee -a "$LOG_FILE"
  echo ""
  log "--- PYTEST: Complete ---"
}

run_canary() {
  log "--- CANARY: Live API Health Check ---"
  echo ""
  python tests/canary_test.py 2>&1 | tee -a "$LOG_FILE"
  echo ""
  log "--- CANARY: Complete ---"
}

check_secrets() {
  log "--- SECRETS: Environment Check ---"
  echo ""
  python tests/check_secrets.py 2>&1 | tee -a "$LOG_FILE"
  echo ""
  log "--- SECRETS: Complete ---"
}

run_scraper() {
  log "--- SCRAPER: Local Run ---"
  echo ""
  python -m src.main 2>&1 | tee -a "$LOG_FILE"
  echo ""
  log "--- SCRAPER: Complete ---"
}

view_log() {
  echo ""
  if [ -f "$LOG_FILE" ]; then
    echo "=== Last 50 lines of $LOG_FILE ==="
    echo ""
    tail -50 "$LOG_FILE"
  else
    echo "No log file found. Run a test first."
  fi
}

clean_cache() {
  log "--- CLEAN: Removing cache & temp files ---"
  find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
  find . -type f -name "*.pyc" -delete 2>/dev/null || true
  rm -rf .pytest_cache storage/ 2>/dev/null || true
  echo "  Cache and temp files removed."
  log "--- CLEAN: Complete ---"
}

touch "$LOG_FILE"
log "Control Panel started."

while true; do
  show_menu
  read -r choice
  echo ""
  case $choice in
    1) run_pytest ;;
    2) run_canary ;;
    3) check_secrets ;;
    4) run_scraper ;;
    5) view_log ;;
    6) clean_cache ;;
    0)
      log "Control Panel exited."
      echo "  Goodbye!"
      exit 0
      ;;
    *)
      echo "  Invalid option. Please try again."
      ;;
  esac
  echo ""
  printf "  Press Enter to return to menu..."
  read -r
done
