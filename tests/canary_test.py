"""
Canary Test: Live API Health Check

Verifies that the Dutchie GraphQL endpoint is reachable and responds
with a valid structure. Does NOT require a real dispensaryId — it sends
a dummy request and checks that the API is alive and returns the expected
GraphQL response format.
"""

import sys
import httpx

GRAPHQL_ENDPOINT = "https://api.dutchie.com/graphql"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "apollographql-client-name": "dutchie-web",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

CANARY_PAYLOAD = {
    "operationName": "GetFilteredProducts",
    "query": """
    query GetFilteredProducts($dispensaryId: String!, $limit: Int, $offset: Int) {
      filteredProducts(dispensaryId: $dispensaryId, limit: $limit, offset: $offset) {
        products {
          id
          Name
        }
      }
    }
    """,
    "variables": {
        "dispensaryId": "000000000000000000000000",
        "limit": 1,
        "offset": 0,
    },
}


def run_canary():
    print("=" * 50)
    print("  CANARY TEST: Dutchie GraphQL API Health Check")
    print("=" * 50)
    print()

    checks_passed = 0
    checks_failed = 0

    print("[1/3] Testing endpoint connectivity...")
    try:
        resp = httpx.post(GRAPHQL_ENDPOINT, json=CANARY_PAYLOAD, headers=HEADERS, timeout=15.0)
        print(f"      Status Code: {resp.status_code}")

        if resp.status_code in (200, 400):
            print("      PASS: Endpoint is reachable (GraphQL may return 400 for invalid queries)")
            checks_passed += 1
        else:
            print(f"      FAIL: Unexpected status code {resp.status_code}")
            checks_failed += 1
    except httpx.RequestError as e:
        print(f"      FAIL: Connection error - {e}")
        checks_failed += 1
        print(f"\nResults: {checks_passed} passed, {checks_failed} failed")
        return checks_failed == 0

    print()
    print("[2/3] Validating response is JSON...")
    try:
        data = resp.json()
        print("      PASS: Valid JSON response")
        checks_passed += 1
    except Exception:
        print("      FAIL: Response is not valid JSON")
        checks_failed += 1
        print(f"\nResults: {checks_passed} passed, {checks_failed} failed")
        return checks_failed == 0

    print()
    print("[3/3] Checking GraphQL response structure...")
    if "data" in data or "errors" in data:
        print("      PASS: Response contains 'data' or 'errors' key (valid GraphQL)")
        checks_passed += 1

        if "errors" in data:
            print(f"      NOTE: API returned errors (expected with dummy ID): {data['errors'][0].get('message', 'unknown')[:80]}")
        if "data" in data:
            products = data.get("data", {}).get("filteredProducts", {}).get("products", [])
            print(f"      NOTE: Products returned: {len(products)}")
    else:
        print("      FAIL: Response missing both 'data' and 'errors' keys")
        checks_failed += 1

    print()
    print("=" * 50)
    print(f"  RESULTS: {checks_passed} passed, {checks_failed} failed")
    if checks_failed == 0:
        print("  STATUS: ALL CHECKS PASSED")
    else:
        print("  STATUS: SOME CHECKS FAILED")
    print("=" * 50)

    return checks_failed == 0


if __name__ == "__main__":
    success = run_canary()
    sys.exit(0 if success else 1)
