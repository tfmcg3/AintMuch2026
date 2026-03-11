# Dutchie Menu Scraper - Apify Actor

## Overview
High-speed, API-based Apify Actor that scrapes dispensary product menus from Dutchie.com. Uses pure Python HTTP requests to extract the internal `dispensaryId` from store HTML, then paginates through Dutchie's GraphQL API to collect all products. Zero browser automation, near-zero compute costs.

## Recent Changes
- 2026-02-11: Initial project scaffolding created (all files)

## Architecture

### Extraction Flow
1. Fetch dispensary HTML page via `httpx`
2. Extract `dispensaryId` using regex patterns on embedded JSON
3. Query `https://api.dutchie.com/graphql` with `GetFilteredProducts` operation
4. Paginate through results using offset-based pagination (page size: 50)
5. Normalize and push products to Apify dataset

### File Structure
```
.actor/
  actor.json           - Apify actor metadata and dataset views
  input_schema.json    - UI configuration with grouped inputs
src/
  main.py              - Core scraper engine (httpx + apify SDK)
tests/
  test_parser.py       - Pytest: data normalization unit tests
  canary_test.py       - Live API health check script
  check_secrets.py     - Environment variable validator
control_panel.sh       - Interactive terminal menu (numbered options)
Makefile               - CLI shortcuts for common dev tasks
Dockerfile             - Lightweight Apify deployment image
requirements.txt       - Python dependencies
.env.example           - Template for environment variables
```

### Key Technologies
- **httpx**: Async HTTP client for all requests
- **apify SDK**: Actor lifecycle, dataset, proxy management
- **pytest**: Unit testing framework
- **GraphQL**: Direct API queries (no browser rendering)

## User Preferences
- No browser automation (Playwright/Selenium/Puppeteer forbidden)
- API-based architecture only
- Professional Apify UI with grouped input sections
- Interactive terminal control panel for local development
