import asyncio
import re
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from apify import Actor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dutchie-scraper")

GRAPHQL_ENDPOINT = "https://api.dutchie.com/graphql"

GRAPHQL_QUERY = """
query GetFilteredProducts($dispensaryId: String!, $limit: Int, $offset: Int) {
  filteredProducts(dispensaryId: $dispensaryId, limit: $limit, offset: $offset) {
    products {
      id
      Name
      brand { name }
      category
      subcategory
      strainType
      Prices
      thc
      cbd
      isSoldOut
    }
  }
}
"""

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "apollographql-client-name": "dutchie-web",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def normalize_product(raw: dict, dispensary_name: str) -> dict:
    brand_data = raw.get("brand")
    brand_name = brand_data.get("name", "") if isinstance(brand_data, dict) else ""

    return {
        "dispensary_name": dispensary_name,
        "product_id": raw.get("id", ""),
        "name": raw.get("Name", ""),
        "brand": brand_name,
        "category": raw.get("category", ""),
        "subcategory": raw.get("subcategory", ""),
        "strain_type": raw.get("strainType", ""),
        "prices": raw.get("Prices", []),
        "thc": raw.get("thc", ""),
        "cbd": raw.get("cbd", ""),
        "is_sold_out": raw.get("isSoldOut", False),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


async def extract_dispensary_id(client: httpx.AsyncClient, url: str) -> str | None:
    logger.info(f"Fetching dispensary page: {url}")
    resp = await client.get(url, headers={"User-Agent": DEFAULT_HEADERS["User-Agent"]}, follow_redirects=True)
    resp.raise_for_status()
    html = resp.text

    patterns = [
        r'"dispensaryId"\s*:\s*"([a-f0-9]{24})"',
        r'"id"\s*:\s*"([a-f0-9]{24})"',
        r'dispensary["\s]*[,:]\s*\{[^}]*"id"\s*:\s*"([a-f0-9]{24})"',
    ]

    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            dispensary_id = match.group(1)
            logger.info(f"Found dispensaryId: {dispensary_id}")
            return dispensary_id

    logger.error(f"Could not extract dispensaryId from {url}")
    return None


def extract_dispensary_name(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    slug = parts[-1] if parts else "unknown"
    return slug.replace("-", " ").title()


async def fetch_products(
    client: httpx.AsyncClient,
    dispensary_id: str,
    page_size: int = 50,
    max_products: int = 0,
) -> list[dict]:
    all_products = []
    offset = 0

    while True:
        payload = {
            "operationName": "GetFilteredProducts",
            "query": GRAPHQL_QUERY,
            "variables": {
                "dispensaryId": dispensary_id,
                "limit": page_size,
                "offset": offset,
            },
        }

        logger.info(f"Fetching products offset={offset}, limit={page_size}")
        resp = await client.post(GRAPHQL_ENDPOINT, json=payload, headers=DEFAULT_HEADERS)
        resp.raise_for_status()

        data = resp.json()
        products = (
            data.get("data", {})
            .get("filteredProducts", {})
            .get("products", [])
        )

        if not products:
            logger.info("No more products returned. Pagination complete.")
            break

        all_products.extend(products)
        logger.info(f"Fetched {len(products)} products (total: {len(all_products)})")

        if max_products > 0 and len(all_products) >= max_products:
            all_products = all_products[:max_products]
            logger.info(f"Reached max product limit: {max_products}")
            break

        if len(products) < page_size:
            logger.info("Last page reached (partial page returned).")
            break

        offset += page_size

    return all_products


async def scrape_dispensary(
    client: httpx.AsyncClient,
    url: str,
    page_size: int,
    max_products: int,
) -> list[dict]:
    dispensary_name = extract_dispensary_name(url)
    dispensary_id = await extract_dispensary_id(client, url)

    if not dispensary_id:
        logger.error(f"Skipping {url}: could not extract dispensaryId")
        return []

    raw_products = await fetch_products(client, dispensary_id, page_size, max_products)

    normalized = [normalize_product(p, dispensary_name) for p in raw_products]
    logger.info(f"Normalized {len(normalized)} products from {dispensary_name}")
    return normalized


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        start_urls = actor_input.get("startUrls", [])
        max_products = actor_input.get("maxProducts", 0)
        page_size = min(actor_input.get("pageSize", 50), 50)

        proxy_config = actor_input.get("proxyConfiguration")
        proxy_url = None
        if proxy_config:
            proxy_url = await Actor.create_proxy_configuration(
                actor_proxy_input=proxy_config
            )

        if not start_urls:
            logger.warning("No startUrls provided. Exiting.")
            return

        urls = [item.get("url", item) if isinstance(item, dict) else item for item in start_urls]

        transport_kwargs = {}
        if proxy_url:
            transport_kwargs["proxy"] = await proxy_url.new_url()

        async with httpx.AsyncClient(timeout=30.0, **transport_kwargs) as client:
            for url in urls:
                logger.info(f"Processing dispensary: {url}")
                try:
                    products = await scrape_dispensary(client, url, page_size, max_products)
                    if products:
                        await Actor.push_data(products)
                        logger.info(f"Pushed {len(products)} products to dataset")
                    else:
                        logger.warning(f"No products found for {url}")
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error for {url}: {e.response.status_code}")
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")

        logger.info("Scraper run complete.")


if __name__ == "__main__":
    asyncio.run(main())
