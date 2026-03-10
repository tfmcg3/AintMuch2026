"""
================================================================================
  Dutchie Menu Scraper — Apify Actor (main.py)
================================================================================

  Architecture (validated against live Dutchie API, March 2026):

    1. Accept input from Apify (single or bulk dispensary URLs)
    2. Extract the URL slug from each Dutchie URL
    3. Resolve the slug to a dispensaryId via the ConsumerDispensaries
       persisted query on /graphql (endpoint #1)
    4. Fetch all products via the FilteredProducts persisted query
       on /api-2/graphql (endpoint #2) with page-based pagination
    5. Pipe raw results through the Golden Schema normalization pipeline
    6. Push clean data to the Apify dataset

  Key Technical Details:
    - Dutchie uses TWO separate GraphQL endpoints
    - All queries use persisted query hashes (GET requests), not raw GraphQL
    - curl_cffi with Chrome TLS fingerprint bypasses Cloudflare
    - The URL slug (e.g. "quincy-cannabis-co") may differ from the internal
      cName (e.g. "quincy-cannabis-quincy-retail-rec"); we handle both
    - Pagination is page-based (page=0, perPage=50), NOT offset-based

================================================================================
"""

import asyncio
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urlparse

from curl_cffi import requests as cffi_requests

# Apify SDK import — graceful fallback for local testing
try:
    from apify import Actor
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False

from golden_schema import GoldenSchemaPipeline

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dutchie-scraper")


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — Validated against live Dutchie API (March 2026)
# ══════════════════════════════════════════════════════════════════════════════

# Endpoint #1: Dispensary lookup (ConsumerDispensaries)
DISPENSARY_ENDPOINT = "https://dutchie.com/graphql"

# Endpoint #2: Product queries (FilteredProducts)
PRODUCTS_ENDPOINT = "https://dutchie.com/api-2/graphql"

# Persisted query hashes (captured from live Dutchie frontend)
HASH_CONSUMER_DISPENSARIES = "0d3ff8648848a737bbbeff9d090854ce2b78a7c4330d4982dab5b32ba2009448"
HASH_FILTERED_PRODUCTS = "c3dda0418c4b423ed26a38d011b50a2b8c9a1f8bde74b45f93420d60d2c50ae1"

# Required headers for Dutchie's Apollo/GraphQL gateway
BASE_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "apollo-require-preflight": "true",
    "Origin": "https://dutchie.com",
    "Referer": "https://dutchie.com/",
}

# Pagination & rate limiting
PAGE_SIZE = 50           # Max products per page (Dutchie's limit)
MAX_PAGES = 100          # Safety cap: 100 pages × 50 = 5,000 products
MAX_RETRIES = 3          # Retries per request
RETRY_BACKOFF = 2.0      # Exponential backoff base (seconds)
REQUEST_DELAY = 0.5      # Delay between paginated requests (seconds)


# ══════════════════════════════════════════════════════════════════════════════
# URL PARSING
# ══════════════════════════════════════════════════════════════════════════════

def extract_slug_from_url(url: str) -> str:
    """
    Extract the dispensary slug from a Dutchie URL.

    Handles:
      https://dutchie.com/dispensary/store-name
      https://dutchie.com/dispensary/store-name/menu
      https://dutchie.com/embedded-menu/store-name
      https://www.dutchie.com/dispensary/store-name?ref=abc

    Returns the slug string (e.g., 'store-name') or raises ValueError.
    """
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid URL: {url}")

    url = url.strip().rstrip("/")
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    segments = [s for s in path.split("/") if s]

    # Pattern: /dispensary/{slug} or /dispensary/{slug}/menu
    # Pattern: /embedded-menu/{slug}
    if len(segments) >= 2:
        prefix = segments[0].lower()
        if prefix in ("dispensary", "embedded-menu"):
            slug = segments[1]
            if re.match(r"^[a-zA-Z0-9][-a-zA-Z0-9]*$", slug):
                return slug

    # Fallback: try the last meaningful segment
    if segments:
        candidate = segments[-1] if segments[-1] != "menu" else (
            segments[-2] if len(segments) >= 2 else ""
        )
        if candidate and re.match(r"^[a-zA-Z0-9][-a-zA-Z0-9]*$", candidate):
            logger.warning(f"Non-standard URL format, extracted slug: {candidate}")
            return candidate

    raise ValueError(
        f"Could not extract dispensary slug from URL: {url}\n"
        f"Expected format: https://dutchie.com/dispensary/your-store-name"
    )


# ══════════════════════════════════════════════════════════════════════════════
# HTTP CLIENT — curl_cffi with Chrome TLS fingerprint
# ══════════════════════════════════════════════════════════════════════════════

class DutchieClient:
    """
    HTTP client for Dutchie's GraphQL API.
    Uses curl_cffi to impersonate Chrome's TLS fingerprint, which is required
    to bypass Cloudflare's bot detection on Dutchie's endpoints.
    """

    def __init__(self, proxy_url: str | None = None):
        self.session = cffi_requests.Session(impersonate="chrome")
        self.proxy_url = proxy_url

    def _get(self, url: str, params: dict, headers: dict, timeout: int = 30) -> dict:
        """Execute a GET request with retry logic."""
        kwargs = {
            "params": params,
            "headers": headers,
            "impersonate": "chrome",
            "timeout": timeout,
        }
        if self.proxy_url:
            kwargs["proxies"] = {"https": self.proxy_url, "http": self.proxy_url}

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, **kwargs)

                if resp.status_code == 200:
                    return resp.json()

                elif resp.status_code == 429:
                    wait = RETRY_BACKOFF * (2 ** attempt) * 2
                    logger.warning(
                        f"Rate limited (429). Waiting {wait:.1f}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                elif resp.status_code in (502, 503, 504):
                    wait = RETRY_BACKOFF * (2 ** attempt)
                    logger.warning(
                        f"Server error ({resp.status_code}). "
                        f"Retry {attempt + 1}/{MAX_RETRIES} in {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                elif resp.status_code == 403:
                    logger.error(
                        f"403 Forbidden. Cloudflare may have blocked this request. "
                        f"Try enabling Apify proxy."
                    )
                    raise RuntimeError(f"403 Forbidden — Cloudflare block")

                else:
                    logger.error(
                        f"Unexpected HTTP {resp.status_code}: "
                        f"{resp.text[:200]}"
                    )
                    raise RuntimeError(f"HTTP {resp.status_code}")

            except (ConnectionError, TimeoutError, OSError) as e:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(
                    f"Connection error: {e}. "
                    f"Retry {attempt + 1}/{MAX_RETRIES} in {wait:.1f}s"
                )
                last_error = e
                time.sleep(wait)
                continue

        raise RuntimeError(
            f"All {MAX_RETRIES} retries exhausted. Last error: {last_error}"
        )

    def close(self):
        """Close the session."""
        try:
            self.session.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# DISPENSARY RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def resolve_dispensary(client: DutchieClient, slug: str) -> dict:
    """
    Resolve a URL slug to a dispensary record (id, name, cName).

    The URL slug (e.g., "quincy-cannabis-co") may differ from the internal
    cName (e.g., "quincy-cannabis-quincy-retail-rec"). This function tries
    the slug first via ConsumerDispensaries, then falls back to using
    the slug as a direct dispensaryId.

    Returns a dict with keys: id, name, cName
    Raises RuntimeError if the dispensary cannot be found.
    """
    headers = {
        **BASE_HEADERS,
        "x-apollo-operation-name": "ConsumerDispensaries",
    }

    params = {
        "operationName": "ConsumerDispensaries",
        "variables": json.dumps({"dispensaryFilter": {"cNameOrID": slug}}),
        "extensions": json.dumps({
            "persistedQuery": {
                "version": 1,
                "sha256Hash": HASH_CONSUMER_DISPENSARIES,
            }
        }),
    }

    logger.info(f"[{slug}] Resolving dispensary via ConsumerDispensaries...")

    data = client._get(DISPENSARY_ENDPOINT, params=params, headers=headers)

    dispensaries = data.get("data", {}).get("filteredDispensaries", [])

    if dispensaries:
        disp = dispensaries[0]
        result = {
            "id": disp.get("id", ""),
            "name": disp.get("name", ""),
            "cName": disp.get("cName", slug),
        }
        logger.info(
            f"[{slug}] Resolved: {result['name']} "
            f"(id={result['id']}, cName={result['cName']})"
        )
        return result

    # Slug didn't match as cName — maybe it's a vanity URL.
    # Try using the slug directly as a dispensaryId (some are hex IDs).
    if re.match(r"^[0-9a-fA-F]{24}$", slug):
        logger.info(f"[{slug}] Slug looks like a dispensaryId. Trying direct lookup...")
        params["variables"] = json.dumps({"dispensaryFilter": {"cNameOrID": slug}})
        data2 = client._get(DISPENSARY_ENDPOINT, params=params, headers=headers)
        dispensaries2 = data2.get("data", {}).get("filteredDispensaries", [])
        if dispensaries2:
            disp = dispensaries2[0]
            return {
                "id": disp.get("id", ""),
                "name": disp.get("name", ""),
                "cName": disp.get("cName", slug),
            }

    raise RuntimeError(
        f"Could not find dispensary for slug '{slug}'. "
        f"This URL slug may be a vanity URL that doesn't match the internal cName. "
        f"Try using the dispensary's actual Dutchie cName or ID instead. "
        f"You can find the correct cName by searching for the dispensary on dutchie.com "
        f"and checking the URL after the page fully loads."
    )


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT FETCHING
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_products(
    client: DutchieClient,
    dispensary_id: str,
    slug: str,
    max_items: int = 0,
    pricing_type: str = "rec",
) -> list[dict]:
    """
    Fetch all products from a dispensary using the FilteredProducts
    persisted query on /api-2/graphql.

    Uses page-based pagination (page=0, perPage=50).
    Returns a list of raw product dicts from the GraphQL API.
    """
    headers = {
        **BASE_HEADERS,
        "x-apollo-operation-name": "FilteredProducts",
        "Referer": f"https://dutchie.com/dispensary/{slug}",
    }

    all_products = []

    for page_num in range(MAX_PAGES):
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId": dispensary_id,
                "pricingType": pricing_type,
                "strainTypes": [],
                "subcategories": [],
                "Status": "Active",
                "types": [],
                "useCache": False,
                "isDefaultSort": True,
                "sortBy": "popularSortIdx",
                "sortDirection": 1,
                "bypassOnlineThresholds": False,
                "isKioskMenu": False,
                "removeProductsBelowOptionThresholds": True,
                "platformType": "ONLINE_MENU",
                "preOrderType": None,
            },
            "page": page_num,
            "perPage": PAGE_SIZE,
        }

        params = {
            "operationName": "FilteredProducts",
            "variables": json.dumps(variables),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": HASH_FILTERED_PRODUCTS,
                }
            }),
        }

        logger.info(f"[{slug}] Fetching page {page_num + 1} (perPage={PAGE_SIZE})...")

        try:
            data = client._get(PRODUCTS_ENDPOINT, params=params, headers=headers)
        except RuntimeError as e:
            logger.error(f"[{slug}] Request failed on page {page_num + 1}: {e}")
            break

        # Check for GraphQL errors
        if "errors" in data:
            error_msg = data["errors"][0].get("message", "Unknown error")
            logger.error(f"[{slug}] GraphQL error: {error_msg}")
            break

        # Extract products
        filtered = data.get("data", {}).get("filteredProducts", {})
        products = filtered.get("products", [])

        logger.info(f"[{slug}] Page {page_num + 1}: {len(products)} products")

        if not products:
            logger.info(f"[{slug}] No more products. Pagination complete.")
            break

        all_products.extend(products)

        # Check max_items limit
        if max_items > 0 and len(all_products) >= max_items:
            all_products = all_products[:max_items]
            logger.info(f"[{slug}] Reached maxItems limit ({max_items}). Stopping.")
            break

        # If we got fewer than a full page, we're done
        if len(products) < PAGE_SIZE:
            logger.info(
                f"[{slug}] Last page reached "
                f"({len(products)} < {PAGE_SIZE})."
            )
            break

        # Rate limit delay between pages
        time.sleep(REQUEST_DELAY)

    logger.info(f"[{slug}] Total raw products collected: {len(all_products)}")
    return all_products


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ACTOR ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def run_actor():
    """Main Apify Actor logic."""
    if APIFY_AVAILABLE:
        async with Actor:
            actor_input = await Actor.get_input() or {}
            await _process_input(actor_input)
    else:
        # Local testing mode
        logger.info("Running in LOCAL mode (Apify SDK not available)")
        test_input = {
            "scrapingMode": "single",
            "startUrls": [
                {"url": "https://dutchie.com/dispensary/pinnacle-cannabis-quincy"}
            ],
            "maxItems": 20,
        }
        await _process_input(test_input)


async def _process_input(actor_input: dict):
    """Process the actor input and scrape all requested dispensaries."""
    # Parse input
    mode = actor_input.get("scrapingMode", "single")
    raw_urls = actor_input.get("startUrls", [])
    max_items = actor_input.get("maxItems", 0)
    use_proxy = actor_input.get("useProxy", False)
    proxy_group = actor_input.get("proxyGroup", "RESIDENTIAL")

    # Normalize URL list
    urls = []
    for item in raw_urls:
        if isinstance(item, dict):
            url = item.get("url", "")
        elif isinstance(item, str):
            url = item
        else:
            continue
        if url:
            urls.append(url.strip())

    if not urls:
        logger.error(
            "No URLs provided. Please add at least one Dutchie dispensary URL."
        )
        return

    # In single mode, only process the first URL
    if mode == "single":
        urls = [urls[0]]
        logger.info("Single mode: processing 1 URL")
    else:
        logger.info(f"Bulk mode: processing {len(urls)} URLs")

    # Configure proxy if requested
    proxy_url = None
    if use_proxy and APIFY_AVAILABLE:
        try:
            proxy_config = await Actor.create_proxy_configuration(
                groups=[proxy_group]
            )
            proxy_url = await proxy_config.new_url()
            logger.info(f"Proxy enabled: {proxy_group}")
        except Exception as e:
            logger.warning(
                f"Could not configure proxy: {e}. Proceeding without proxy."
            )

    # Create the HTTP client
    client = DutchieClient(proxy_url=proxy_url)

    # Process each dispensary
    total_products_pushed = 0

    try:
        for i, url in enumerate(urls):
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing [{i + 1}/{len(urls)}]: {url}")
            logger.info(f"{'=' * 60}")

            # Step 1: Extract slug from URL
            try:
                slug = extract_slug_from_url(url)
                logger.info(f"URL slug: {slug}")
            except ValueError as e:
                logger.error(f"Skipping invalid URL: {e}")
                continue

            # Step 2: Resolve dispensary (slug → id + name)
            try:
                disp_info = resolve_dispensary(client, slug)
            except RuntimeError as e:
                logger.error(f"[{slug}] {e}")
                continue

            dispensary_id = disp_info["id"]
            dispensary_name = disp_info["name"]
            dispensary_cname = disp_info["cName"]

            # Step 3: Fetch all products
            try:
                raw_products = fetch_all_products(
                    client=client,
                    dispensary_id=dispensary_id,
                    slug=dispensary_cname,
                    max_items=max_items,
                )
            except Exception as e:
                logger.error(f"[{slug}] Scraping failed: {e}")
                continue

            if not raw_products:
                logger.warning(
                    f"[{slug}] No products returned. "
                    f"The store may be offline or the URL may be incorrect."
                )
                continue

            # Step 4: Run through Golden Schema pipeline
            pipeline = GoldenSchemaPipeline(dispensary_slug=dispensary_cname)
            scraped_at = datetime.now(timezone.utc).isoformat()
            clean_products = pipeline.process(
                raw_items=raw_products,
                source_url=url,
                scraped_at=scraped_at,
            )

            if not clean_products:
                logger.warning(
                    f"[{slug}] All products were filtered out "
                    f"during normalization."
                )
                continue

            # Step 5: Push to Apify dataset
            clean_dicts = pipeline.to_dicts()
            if APIFY_AVAILABLE:
                await Actor.push_data(clean_dicts)
                logger.info(
                    f"[{slug}] Pushed {len(clean_dicts)} products "
                    f"to Apify dataset"
                )
            else:
                # Local mode: print summary
                logger.info(
                    f"[{slug}] {len(clean_dicts)} products ready "
                    f"(local mode — not pushed)"
                )
                for p in clean_products[:5]:
                    logger.info(
                        f"  [{p.category:12}] {p.product_name:45} | "
                        f"Brand: {p.brand:20} | "
                        f"THC: {p.thc_percentage}% | "
                        f"${p.price}"
                    )
                if len(clean_products) > 5:
                    logger.info(
                        f"  ... and {len(clean_products) - 5} more products"
                    )

            total_products_pushed += len(clean_dicts)
            stats = pipeline.get_stats()
            logger.info(
                f"[{slug}] Pipeline stats: "
                f"{stats['raw_count']} raw → "
                f"{stats['clean_count']} clean | "
                f"{stats['fingerprint_dupes']} fp-dupes | "
                f"{stats['id_dupes']} id-dupes | "
                f"{stats['invalid_skipped']} skipped"
            )

            # Delay between stores in bulk mode
            if len(urls) > 1 and i < len(urls) - 1:
                time.sleep(1.0)

    finally:
        client.close()

    logger.info(f"\n{'=' * 60}")
    logger.info(
        f"COMPLETE: {total_products_pushed} total products "
        f"across {len(urls)} store(s)"
    )
    logger.info(f"{'=' * 60}")


# ── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    """Synchronous entrypoint for Apify Docker container."""
    asyncio.run(run_actor())


if __name__ == "__main__":
    main()
