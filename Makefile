.PHONY: test canary secrets run clean help

help:
	@echo "========================================="
	@echo "  Dutchie Menu Scraper - Dev Commands"
	@echo "========================================="
	@echo ""
	@echo "  make test      Run pytest data normalization tests"
	@echo "  make canary    Run live API canary health check"
	@echo "  make secrets   Verify Replit environment variables"
	@echo "  make run       Run the scraper locally"
	@echo "  make clean     Remove cache and temp files"
	@echo "  make help      Show this help message"
	@echo ""

test:
	@echo "[$(shell date)] Running pytest..." | tee -a dev_journal.log
	python -m pytest tests/test_parser.py -v 2>&1 | tee -a dev_journal.log

canary:
	@echo "[$(shell date)] Running canary test..." | tee -a dev_journal.log
	python tests/canary_test.py 2>&1 | tee -a dev_journal.log

secrets:
	@echo "[$(shell date)] Checking secrets..." | tee -a dev_journal.log
	python tests/check_secrets.py 2>&1 | tee -a dev_journal.log

run:
	@echo "[$(shell date)] Running scraper locally..." | tee -a dev_journal.log
	python -m src.main 2>&1 | tee -a dev_journal.log

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache storage/ 2>/dev/null || true
	@echo "Done."
