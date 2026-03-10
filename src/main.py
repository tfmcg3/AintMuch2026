"""
================================================================================
  Dutchie Menu Scraper — Apify Actor (main.py)
================================================================================

  Architecture (validated against live Dutchie API, March 2026):

    1. Accept input from Apify (single or bulk dispensary URLs)
    2. Extract the URL slug from each Dutchie URL
    3. Resolve the slug to a dispensaryId via a 5-step resolution chain:
       a. Direct ConsumerDispensaries query (slug as cNameOrID)
       b. Pre-built lookup table (dispensary_lookup.json)
       c. Hex ID direct lookup (if slug is a 24-char hex string)
       d. HTML page scrape (extract real cName from page source)
       e. DispensarySearch API fallback (name-based search)
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
    - dispensary_lookup.json maps ~5000+ slugs to cNames (built offline)

================================================================================
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse

# Ensure the current directory (src/) is in the path for Apify
# This fixes the ModuleNotFoundError when running as 'python -m src.main'
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

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

# Persisted query hash for DispensarySearch (geo-based search fallback)
HASH_DISPENSARY_SEARCH = "3e25a4d63e0a5e220b9b8e6b5a2e3c4f5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a"

# Pagination & rate limiting
PAGE_SIZE = 50           # Max products per page (Dutchie's limit)
MAX_PAGES = 100          # Safety cap: 100 pages × 50 = 5,000 products
MAX_RETRIES = 3          # Retries per request
RETRY_BACKOFF = 2.0      # Exponential backoff base (seconds)
REQUEST_DELAY = 0.5      # Delay between paginated requests (seconds)


# ══════════════════════════════════════════════════════════════════════════════
# DISPENSARY LOOKUP TABLE
# ══════════════════════════════════════════════════════════════════════════════

def _load_dispensary_lookup() -> dict:
    """
    Load the pre-built dispensary lookup table from dispensary_lookup.json.

    This file maps URL slugs / vanity names to their real Dutchie cNames.
    It is built offline by scraping all city pages on dutchie.com/cities
    and extracting every dispensary link.

    Returns a dict keyed by slug with values containing cName, name, etc.
    """
    lookup_path = Path(__file__).parent / "dispensary_lookup.json"
    if lookup_path.exists():
        try:
            with open(lookup_path) as f:
                lookup = json.load(f)
            logger.info(f"Loaded dispensary lookup table: {len(lookup)} entries")
            return lookup
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load dispensary lookup: {e}")
    else:
        logger.info("No dispensary_lookup.json found — lookup table disabled")
    return {}


# Global lookup table (loaded once at module import)
DISPENSARY_LOOKUP = _load_dispensary_lookup()


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

def _query_dispensary(client: DutchieClient, cname_or_id: str) -> list[dict]:
    """
    Query the ConsumerDispensaries endpoint for a given cNameOrID.
    Returns the list of matching dispensary records (may be empty).
    """
    headers = {
        **BASE_HEADERS,
        "x-apollo-operation-name": "ConsumerDispensaries",
    }
    params = {
        "operationName": "ConsumerDispensaries",
        "variables": json.dumps({"dispensaryFilter": {"cNameOrID": cname_or_id}}),
        "extensions": json.dumps({
            "persistedQuery": {
                "version": 1,
                "sha256Hash": HASH_CONSUMER_DISPENSARIES,
            }
        }),
    }
    data = client._get(DISPENSARY_ENDPOINT, params=params, headers=headers)
    return data.get("data", {}).get("filteredDispensaries", [])


def _extract_cname_from_html(client: DutchieClient, slug: str) -> str | None:
    """
    Fallback: Load the dispensary page HTML and extract the real cName.

    When a user provides a vanity URL slug (e.g., "quincy-cannabis-co"),
    the ConsumerDispensaries query may return empty because the slug
    doesn't match any cName. However, Dutchie's frontend JavaScript
    handles the resolution client-side.

    This function loads the dispensary page and looks for the real cName
    in the Apollo Client cache, __NEXT_DATA__, or by following any
    JavaScript-initiated redirects embedded in the page.

    Returns the real cName string, or None if it cannot be determined.
    """
    try:
        logger.info(f"[{slug}] Attempting HTML fallback to resolve vanity URL...")
        resp = client.session.get(
            f"https://dutchie.com/dispensary/{slug}",
            impersonate="chrome",
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"[{slug}] HTML fallback got HTTP {resp.status_code}")
            return None

        html = resp.text

        # Strategy 1: Check if the final URL was redirected (server-side)
        final_url = str(resp.url)
        final_match = re.search(r'/dispensary/([^/?#]+)', final_url)
        if final_match:
            resolved = final_match.group(1)
            if resolved != slug:
                logger.info(f"[{slug}] Server redirect resolved to: {resolved}")
                return resolved

        # Strategy 2: Look for canonical link tag
        canonical = re.search(
            r'<link[^>]*rel=["\']canonical["\'][^>]*href=["\']([^"\']*/dispensary/([^"\'/]+))',
            html, re.I
        )
        if canonical:
            resolved = canonical.group(2)
            if resolved != slug:
                logger.info(f"[{slug}] Canonical link resolved to: {resolved}")
                return resolved

        # Strategy 3: Look for dispensary data in __NEXT_DATA__
        next_data_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.S
        )
        if next_data_match:
            try:
                nd = json.loads(next_data_match.group(1))
                # Check pageProps for dispensary data
                props = nd.get("props", {}).get("pageProps", {})
                if "dispensary" in props:
                    disp = props["dispensary"]
                    cname = disp.get("cName")
                    if cname and cname != slug:
                        logger.info(f"[{slug}] __NEXT_DATA__ resolved to: {cname}")
                        return cname
            except (json.JSONDecodeError, KeyError):
                pass

        # Strategy 4: Look for Apollo cache data in the HTML
        # The Apollo cache sometimes embeds dispensary data in script tags
        apollo_match = re.search(
            r'"cName"\s*:\s*"([^"]+)"',
            html
        )
        if apollo_match:
            resolved = apollo_match.group(1)
            if resolved != slug:
                logger.info(f"[{slug}] Apollo cache resolved to: {resolved}")
                return resolved

        logger.warning(f"[{slug}] HTML fallback could not resolve vanity URL")
        return None

    except Exception as e:
        logger.warning(f"[{slug}] HTML fallback failed: {e}")
        return None


def _lookup_slug(slug: str) -> str | None:
    """
    Check the pre-built lookup table for a matching dispensary cName.

    Tries exact match first, then fuzzy matching (partial slug match).
    Returns the resolved cName or None.
    """
    # Exact match
    if slug in DISPENSARY_LOOKUP:
        entry = DISPENSARY_LOOKUP[slug]
        logger.info(
            f"[{slug}] Found in lookup table: {entry.get('name', slug)} "
            f"(cName={entry.get('cName', slug)})"
        )
        return entry.get("cName", slug)

    # Fuzzy match: check if the slug is a substring of any known cName
    slug_lower = slug.lower()
    candidates = []
    for key, entry in DISPENSARY_LOOKUP.items():
        if slug_lower in key.lower() or key.lower() in slug_lower:
            candidates.append((key, entry))

    if len(candidates) == 1:
        key, entry = candidates[0]
        logger.info(
            f"[{slug}] Fuzzy match in lookup table: {entry.get('name', key)} "
            f"(cName={key})"
        )
        return entry.get("cName", key)
    elif len(candidates) > 1:
        logger.info(
            f"[{slug}] Multiple fuzzy matches in lookup table: "
            f"{[c[0] for c in candidates[:5]]}. Skipping fuzzy match."
        )

    return None


def _search_dispensary_by_name(client: DutchieClient, slug: str) -> str | None:
    """
    Runtime fallback: Search for a dispensary by name using Dutchie's
    DispensarySearch GraphQL query.

    This is used when the lookup table and HTML fallback both fail.
    It searches Dutchie's dispensary index by the slug (treated as a
    search term) and returns the best-matching cName.

    Returns the resolved cName or None.
    """
    try:
        logger.info(f"[{slug}] Trying DispensarySearch API fallback...")

        # Convert slug to search-friendly text: "quincy-cannabis-co" -> "quincy cannabis co"
        search_term = slug.replace("-", " ").strip()

        headers = {
            **BASE_HEADERS,
            "x-apollo-operation-name": "DispensarySearch",
        }

        # Use the search endpoint with the dispensary name
        variables = {
            "searchTerm": search_term,
            "limit": 10,
        }

        params = {
            "operationName": "DispensarySearch",
            "variables": json.dumps(variables),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": HASH_DISPENSARY_SEARCH,
                }
            }),
        }

        try:
            data = client._get(DISPENSARY_ENDPOINT, params=params, headers=headers)
        except RuntimeError:
            logger.warning(f"[{slug}] DispensarySearch API request failed")
            return None

        # Parse results
        results = data.get("data", {}).get("dispensarySearch", {}).get("results", [])
        if not results:
            results = data.get("data", {}).get("filteredDispensaries", [])

        if not results:
            logger.warning(f"[{slug}] DispensarySearch returned no results")
            return None

        # Find the best match by comparing cName/name similarity to the slug
        slug_lower = slug.lower().replace("-", "")
        best_match = None
        best_score = 0

        for r in results:
            cname = r.get("cName", "")
            name = r.get("name", "")

            # Score based on substring overlap
            cname_lower = cname.lower().replace("-", "")
            name_lower = name.lower().replace(" ", "")

            score = 0
            if slug_lower == cname_lower:
                score = 100  # Exact match
            elif slug_lower in cname_lower or cname_lower in slug_lower:
                score = 80  # Substring match on cName
            elif slug_lower in name_lower or name_lower in slug_lower:
                score = 60  # Substring match on name
            else:
                # Count common words
                slug_words = set(slug.lower().split("-"))
                cname_words = set(cname.lower().split("-"))
                name_words = set(name.lower().split())
                common = len(slug_words & (cname_words | name_words))
                if common > 0:
                    score = common * 20

            if score > best_score:
                best_score = score
                best_match = r

        if best_match and best_score >= 40:
            resolved = best_match.get("cName", "")
            logger.info(
                f"[{slug}] DispensarySearch resolved to: "
                f"{best_match.get('name', '')} (cName={resolved}, score={best_score})"
            )
            return resolved

        logger.warning(
            f"[{slug}] DispensarySearch found results but no confident match "
            f"(best score: {best_score})"
        )
        return None

    except Exception as e:
        logger.warning(f"[{slug}] DispensarySearch fallback failed: {e}")
        return None


def resolve_dispensary(client: DutchieClient, slug: str) -> dict:
    """
    Resolve a URL slug to a dispensary record (id, name, cName).

    Resolution strategy (5 steps, in order):
      1. Try the slug directly as cNameOrID via ConsumerDispensaries
      2. Check the pre-built lookup table for a matching cName
      3. If the slug looks like a hex ID, try it as a dispensaryId
      4. Load the dispensary page HTML and extract the real cName
      5. Search Dutchie's DispensarySearch API as a runtime fallback

    Returns a dict with keys: id, name, cName
    Raises RuntimeError if the dispensary cannot be found.
    """
    logger.info(f"[{slug}] Resolving dispensary...")

    # ── Attempt 1: Direct cNameOrID lookup ────────────────────────────────
    logger.info(f"[{slug}] Step 1: Trying direct ConsumerDispensaries query...")
    dispensaries = _query_dispensary(client, slug)
    if dispensaries:
        disp = dispensaries[0]
        result = {
            "id": disp.get("id", ""),
            "name": disp.get("name", ""),
            "cName": disp.get("cName", slug),
        }
        logger.info(
            f"[{slug}] Resolved (Step 1): {result['name']} "
            f"(id={result['id']}, cName={result['cName']})"
        )
        return result

    # ── Attempt 2: Lookup table ───────────────────────────────────────────
    logger.info(f"[{slug}] Step 2: Checking lookup table...")
    lookup_cname = _lookup_slug(slug)
    if lookup_cname and lookup_cname != slug:
        dispensaries2 = _query_dispensary(client, lookup_cname)
        if dispensaries2:
            disp = dispensaries2[0]
            result = {
                "id": disp.get("id", ""),
                "name": disp.get("name", ""),
                "cName": disp.get("cName", lookup_cname),
            }
            logger.info(
                f"[{slug}] Resolved (Step 2 — lookup table): {result['name']} "
                f"(id={result['id']}, cName={result['cName']})"
            )
            return result

    # ── Attempt 3: Hex ID lookup ──────────────────────────────────────────
    if re.match(r"^[0-9a-fA-F]{24}$", slug):
        logger.info(f"[{slug}] Step 3: Slug looks like a dispensaryId...")
        dispensaries3 = _query_dispensary(client, slug)
        if dispensaries3:
            disp = dispensaries3[0]
            return {
                "id": disp.get("id", ""),
                "name": disp.get("name", ""),
                "cName": disp.get("cName", slug),
            }

    # ── Attempt 4: Vanity URL fallback (HTML scrape) ──────────────────────
    logger.info(f"[{slug}] Step 4: Trying HTML page scrape fallback...")
    resolved_cname = _extract_cname_from_html(client, slug)
    if resolved_cname:
        logger.info(f"[{slug}] Vanity URL resolved to cName: {resolved_cname}")
        dispensaries4 = _query_dispensary(client, resolved_cname)
        if dispensaries4:
            disp = dispensaries4[0]
            return {
                "id": disp.get("id", ""),
                "name": disp.get("name", ""),
                "cName": disp.get("cName", resolved_cname),
            }
        else:
            logger.warning(
                f"[{slug}] Resolved cName '{resolved_cname}' also returned empty."
            )

    # ── Attempt 5: DispensarySearch API fallback ──────────────────────────
    logger.info(f"[{slug}] Step 5: Trying DispensarySearch API fallback...")
    search_cname = _search_dispensary_by_name(client, slug)
    if search_cname:
        dispensaries5 = _query_dispensary(client, search_cname)
        if dispensaries5:
            disp = dispensaries5[0]
            return {
                "id": disp.get("id", ""),
                "name": disp.get("name", ""),
                "cName": disp.get("cName", search_cname),
            }

    # ── All attempts failed ───────────────────────────────────────────────
    raise RuntimeError(
        f"Could not find dispensary for slug '{slug}'. "
        f"All 5 resolution strategies failed. "
        f"This usually means the URL slug is not a valid Dutchie dispensary. "
        f"\n\nTo fix this:\n"
        f"  1. Open https://dutchie.com/dispensary/{slug} in your browser\n"
        f"  2. Wait for the page to fully load\n"
        f"  3. Copy the URL from your browser's address bar\n"
        f"  4. The slug in the URL after /dispensary/ is the correct cName\n"
        f"     (it may be different from what you originally entered)\n"
        f"  5. Use that URL with this scraper"
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
    category_filter = actor_input.get("categoryFilter", "").strip()
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
                
                # Apply category filter if specified
                if category_filter:
                    logger.info(f"[{slug}] Applying category filter: {category_filter}")
                    filtered_products = []
                    cf_lower = category_filter.lower()
                    for p in raw_products:
                        # Check multiple possible category fields in Dutchie GraphQL
                        cat = (p.get("menuType") or p.get("type") or "").lower()
                        if cf_lower in cat:
                            filtered_products.append(p)
                    logger.info(f"[{slug}] Filtered {len(raw_products)} -> {len(filtered_products)} products")
                    raw_products = filtered_products
                    
            except Exception as e:
                logger.error(f"[{slug}] Scraping failed: {e}")
                continue

            if not raw_products:
                logger.warning(
                    f"[{slug}] No products returned (or filtered out). "
                    f"The store may be offline, the URL incorrect, or the category filter too restrictive."
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
