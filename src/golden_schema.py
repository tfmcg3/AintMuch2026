"""
================================================================================
  GOLDEN SCHEMA — Dutchie Menu Data Normalization & Deduplication Pipeline
================================================================================

  7-Layer Architecture:
    1. Canonical Lookup Tables (categories, strains, weights)
    2. GoldenProduct Dataclass (the output shape)
    3. Individual Field Normalizers (smart_title, parse_percentage, etc.)
    4. Fingerprint & Deduplication Engine
    5. Raw Field Extractor (handles all Dutchie GraphQL variants)
    6. GoldenSchemaPipeline Class (orchestrator)
    7. Export helpers (CSV, JSON, Apify-ready dicts)

  Usage:
    from golden_schema import GoldenSchemaPipeline

    pipeline = GoldenSchemaPipeline(dispensary_slug="quincy-cannabis-co")
    clean = pipeline.process(raw_graphql_items, source_url=url)
    dicts = pipeline.to_dicts()  # ready for Actor.push_data()
================================================================================
"""

import csv
import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("golden-schema")

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — Canonical Lookup Tables
# ══════════════════════════════════════════════════════════════════════════════

CATEGORY_MAP = {
    # Flower
    "flower":           "Flower",
    "flowers":          "Flower",
    "buds":             "Flower",
    "bud":              "Flower",
    "shake":            "Flower",
    "smalls":           "Flower",
    "ground flower":    "Flower",
    "ground":           "Flower",
    "indoor flower":    "Flower",
    "outdoor flower":   "Flower",
    "greenhouse":       "Flower",
    # Pre-Roll
    "pre-roll":         "Pre-Roll",
    "pre-rolls":        "Pre-Roll",
    "preroll":          "Pre-Roll",
    "prerolls":         "Pre-Roll",
    "pre roll":         "Pre-Roll",
    "pre rolls":        "Pre-Roll",
    "joint":            "Pre-Roll",
    "joints":           "Pre-Roll",
    "blunt":            "Pre-Roll",
    "blunts":           "Pre-Roll",
    "infused pre-roll": "Pre-Roll",
    "infused preroll":  "Pre-Roll",
    "multi-pack":       "Pre-Roll",
    # Concentrate
    "concentrate":      "Concentrate",
    "concentrates":     "Concentrate",
    "extract":          "Concentrate",
    "extracts":         "Concentrate",
    "wax":              "Concentrate",
    "shatter":          "Concentrate",
    "resin":            "Concentrate",
    "rosin":            "Concentrate",
    "live resin":       "Concentrate",
    "live rosin":       "Concentrate",
    "badder":           "Concentrate",
    "budder":           "Concentrate",
    "crumble":          "Concentrate",
    "diamonds":         "Concentrate",
    "sauce":            "Concentrate",
    "hash":             "Concentrate",
    "bubble hash":      "Concentrate",
    "kief":             "Concentrate",
    "rso":              "Concentrate",
    "distillate":       "Concentrate",
    # Vape
    "vape":             "Vape",
    "vapes":            "Vape",
    "vaporizer":        "Vape",
    "vaporizers":       "Vape",
    "cartridge":        "Vape",
    "cartridges":       "Vape",
    "vape cartridge":   "Vape",
    "vape cartridges":  "Vape",
    "vape pen":         "Vape",
    "vape pens":        "Vape",
    "cart":             "Vape",
    "carts":            "Vape",
    "pod":              "Vape",
    "pods":             "Vape",
    "disposable":       "Vape",
    "disposables":      "Vape",
    "disposable vape":  "Vape",
    # Edible
    "edible":           "Edible",
    "edibles":          "Edible",
    "gummy":            "Edible",
    "gummies":          "Edible",
    "chocolate":        "Edible",
    "chocolates":       "Edible",
    "candy":            "Edible",
    "beverage":         "Edible",
    "beverages":        "Edible",
    "drink":            "Edible",
    "drinks":           "Edible",
    "capsule":          "Edible",
    "capsules":         "Edible",
    "tablet":           "Edible",
    "tablets":          "Edible",
    "lozenge":          "Edible",
    "lozenges":         "Edible",
    "mint":             "Edible",
    "mints":            "Edible",
    "baked goods":      "Edible",
    "cooking":          "Edible",
    "food":             "Edible",
    # Topical
    "topical":          "Topical",
    "topicals":         "Topical",
    "cream":            "Topical",
    "lotion":           "Topical",
    "balm":             "Topical",
    "salve":            "Topical",
    "patch":            "Topical",
    "patches":          "Topical",
    "transdermal":      "Topical",
    "bath":             "Topical",
    "bath bomb":        "Topical",
    # Tincture
    "tincture":         "Tincture",
    "tinctures":        "Tincture",
    "oil":              "Tincture",
    "oils":             "Tincture",
    "sublingual":       "Tincture",
    "spray":            "Tincture",
    "drops":            "Tincture",
    # Accessory
    "accessory":        "Accessory",
    "accessories":      "Accessory",
    "gear":             "Accessory",
    "merch":            "Accessory",
    "merchandise":      "Accessory",
    "apparel":          "Accessory",
    "battery":          "Accessory",
    "batteries":        "Accessory",
    "grinder":          "Accessory",
    "pipe":             "Accessory",
    "papers":           "Accessory",
    "rolling papers":   "Accessory",
}

STRAIN_TYPE_MAP = {
    "indica":               "Indica",
    "ind":                  "Indica",
    "indica dominant":      "Indica",
    "indica-dominant":      "Indica",
    "indica dom":           "Indica",
    "sativa":               "Sativa",
    "sat":                  "Sativa",
    "sativa dominant":      "Sativa",
    "sativa-dominant":      "Sativa",
    "sativa dom":           "Sativa",
    "hybrid":               "Hybrid",
    "hyb":                  "Hybrid",
    "balanced":             "Hybrid",
    "balanced hybrid":      "Hybrid",
    "cbd":                  "CBD",
    "high cbd":             "CBD",
    "high-cbd":             "CBD",
    "1:1":                  "CBD",
    "2:1":                  "CBD",
    "":                     "N/A",
}

WEIGHT_TO_GRAMS = {
    "1g":       1.0,
    "1 g":      1.0,
    "1.0g":     1.0,
    "2g":       2.0,
    "2 g":      2.0,
    "3.5g":     3.5,
    "3.5 g":    3.5,
    "7g":       7.0,
    "7 g":      7.0,
    "14g":      14.0,
    "14 g":     14.0,
    "28g":      28.0,
    "28 g":     28.0,
    "1/8 oz":   3.5,
    "1/8oz":    3.5,
    "eighth":   3.5,
    "1/4 oz":   7.0,
    "1/4oz":    7.0,
    "quarter":  7.0,
    "1/2 oz":   14.0,
    "1/2oz":    14.0,
    "half":     14.0,
    "half oz":  14.0,
    "1 oz":     28.0,
    "1oz":      28.0,
    "ounce":    28.0,
    "oz":       28.0,
}

# Cannabis abbreviations to preserve as ALL-CAPS during title casing
PRESERVE_CAPS = {
    "og", "thc", "cbd", "cbg", "cbn", "cbda", "thca", "rso",
    "vg", "pg", "co2", "bho", "ph", "uv", "led", "thcv",
    "xl", "xxl",
}

# Marketing noise words to strip from product names
NOISE_WORDS = {
    "new", "new arrival", "featured", "sale", "hot", "best seller",
    "popular", "limited", "exclusive", "special", "staff pick",
    "top pick", "deal", "promo", "clearance", "on sale",
}


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — GoldenProduct Dataclass
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class GoldenProduct:
    """Canonical output record. Every product exits the pipeline in this shape."""
    product_id:       str
    product_name:     str
    brand:            str
    category:         str
    subcategory:      str
    strain_type:      str
    thc_percentage:   Optional[float]
    cbd_percentage:   Optional[float]
    cbg_percentage:   Optional[float]
    cbn_percentage:   Optional[float]
    weight_grams:     Optional[float]
    price:            Optional[float]
    price_per_gram:   Optional[float]
    in_stock:         bool
    dispensary_slug:  str
    source_url:       str
    scraped_at:       str
    fingerprint:      str

    def to_dict(self) -> dict:
        """Convert to a flat dictionary for Apify dataset push."""
        return asdict(self)

    def to_row(self) -> list:
        """Serialize to a flat list matching CSV/Sheets header order."""
        return [
            self.product_id,
            self.product_name,
            self.brand,
            self.category,
            self.subcategory,
            self.strain_type,
            self.thc_percentage if self.thc_percentage is not None else "",
            self.cbd_percentage if self.cbd_percentage is not None else "",
            self.cbg_percentage if self.cbg_percentage is not None else "",
            self.cbn_percentage if self.cbn_percentage is not None else "",
            self.weight_grams if self.weight_grams is not None else "",
            self.price if self.price is not None else "",
            self.price_per_gram if self.price_per_gram is not None else "",
            self.in_stock,
            self.dispensary_slug,
            self.source_url,
            self.scraped_at,
            self.fingerprint,
        ]


SHEETS_HEADERS = [
    "product_id", "product_name", "brand", "category", "subcategory",
    "strain_type", "thc_percentage", "cbd_percentage", "cbg_percentage", "cbn_percentage", "weight_grams",
    "price", "price_per_gram", "in_stock", "dispensary_slug",
    "source_url", "scraped_at", "fingerprint",
]


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — Individual Field Normalizers
# ══════════════════════════════════════════════════════════════════════════════

def smart_title(text: str) -> str:
    """
    Title-case a string while preserving cannabis all-caps abbreviations
    (e.g. 'OG', 'THC', 'THCA', 'RSO') that Python's .title() would mangle.
    """
    if not text:
        return ""
    words = text.split()
    result = []
    for word in words:
        m = re.match(r"([\w'-]+)(.*)", word)
        if m:
            core, suffix = m.group(1), m.group(2)
            if core.lower() in PRESERVE_CAPS:
                result.append(core.upper() + suffix)
            else:
                result.append(core.capitalize() + suffix)
        else:
            result.append(word.capitalize())
    return " ".join(result)


def normalize_name(raw: str) -> str:
    """Clean a product name: strip noise words, collapse whitespace, smart title."""
    if not raw or not isinstance(raw, str):
        return ""
    text = raw.strip()
    # Remove noise words (case-insensitive, whole-word match)
    for noise in NOISE_WORDS:
        text = re.sub(rf"\b{re.escape(noise)}\b", "", text, flags=re.IGNORECASE)
    # Collapse multiple spaces
    text = re.sub(r"\s{2,}", " ", text).strip()
    # Remove leading/trailing dashes or pipes left after stripping
    text = text.strip("-|– ").strip()
    return smart_title(text) if text else ""


def normalize_brand(raw: str) -> str:
    """Clean a brand name. Returns empty string for null/unknown values."""
    if not raw or not isinstance(raw, str):
        return ""
    text = raw.strip()
    if text.lower() in ("n/a", "na", "none", "unknown", "null", "-", ""):
        return ""
    text = re.sub(r"\s{2,}", " ", text)
    return smart_title(text)


def normalize_category(raw: str) -> tuple[str, str]:
    """
    Map a raw Dutchie category string to a canonical (category, subcategory) tuple.
    The subcategory preserves the original value when the category is successfully mapped.
    """
    if not raw or not isinstance(raw, str):
        return ("Other", "")
    cleaned = raw.strip().lower()
    category = CATEGORY_MAP.get(cleaned)
    if category:
        # If the raw value differs from the canonical, keep it as subcategory
        subcategory = smart_title(raw.strip()) if cleaned != category.lower() else ""
        return (category, subcategory)
    # Fallback: try partial matching
    for key, cat in CATEGORY_MAP.items():
        if key in cleaned or cleaned in key:
            return (cat, smart_title(raw.strip()))
    return ("Other", smart_title(raw.strip()))


def normalize_strain_type(raw: str) -> str:
    """Map raw strain type to canonical value."""
    if not raw or not isinstance(raw, str):
        return "N/A"
    cleaned = raw.strip().lower()
    mapped = STRAIN_TYPE_MAP.get(cleaned)
    if mapped:
        return mapped
    # Partial matching for "indica dominant" variants
    if "indica" in cleaned:
        return "Indica"
    if "sativa" in cleaned:
        return "Sativa"
    if "hybrid" in cleaned:
        return "Hybrid"
    if "cbd" in cleaned:
        return "CBD"
    return "N/A"


def parse_percentage(raw) -> Optional[float]:
    """
    Parse THC/CBD percentage from various formats:
      '21.3%' → 21.3
      '< 25%' → 25.0
      '4.5-5.5%' → 5.0 (takes the midpoint)
      78.2 (raw float) → 78.2
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return round(float(raw), 2)
    if not isinstance(raw, str):
        return None
    text = raw.strip().replace("%", "").replace("<", "").replace(">", "").strip()
    if not text:
        return None
    # Handle ranges like "4.5-5.5"
    range_match = re.match(r"([\d.]+)\s*[-–]\s*([\d.]+)", text)
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        return round((low + high) / 2, 2)
    # Standard numeric
    try:
        return round(float(text), 2)
    except ValueError:
        return None


def parse_price(raw) -> Optional[float]:
    """Parse price from various formats: '$45.00', 45, 'free' → None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return round(val, 2) if val > 0 else None
    if not isinstance(raw, str):
        return None
    text = raw.strip().lower()
    if text in ("free", "n/a", "na", "", "-"):
        return None
    cleaned = re.sub(r"[^0-9.]", "", text)
    if not cleaned:
        return None
    try:
        val = float(cleaned)
        return round(val, 2) if val > 0 else None
    except ValueError:
        return None


def parse_weight(raw) -> Optional[float]:
    """
    Parse weight from various formats:
      '3.5g' → 3.5
      '1/8 oz' → 3.5
      '500mg' → 0.5
      '100mg' → 0.1
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return round(float(raw), 4) if float(raw) > 0 else None
    if not isinstance(raw, str):
        return None
    text = raw.strip().lower()
    if not text:
        return None
    # Check lookup table first
    mapped = WEIGHT_TO_GRAMS.get(text)
    if mapped:
        return mapped
    # Handle milligrams (Dutchie often reports 28g as 28000mg internally)
    mg_match = re.search(r"([\d.]+)\s*mg", text)
    if mg_match:
        val = float(mg_match.group(1))
        # If value is huge (like 28000), it's definitely mg
        return round(val / 1000.0, 4)
    
    # Handle grams
    g_match = re.search(r"([\d.]+)\s*g(?:ram)?s?", text)
    if g_match:
        val = float(g_match.group(1))
        # Heuristic: If value is > 500, it's likely actually mg mislabeled as g
        if val >= 500:
            return round(val / 1000.0, 4)
        return round(val, 4)
    # Handle ounces
    oz_match = re.match(r"([\d.]+)\s*oz", text)
    if oz_match:
        return round(float(oz_match.group(1)) * 28.0, 4)
    # Handle fractions like "1/8"
    frac_match = re.match(r"(\d+)/(\d+)\s*(?:oz)?", text)
    if frac_match:
        num = float(frac_match.group(1))
        den = float(frac_match.group(2))
        if den > 0:
            return round((num / den) * 28.0, 4)
    return None


def compute_price_per_gram(price: Optional[float], weight_grams: Optional[float]) -> Optional[float]:
    """Calculate price per gram. Zero-division safe."""
    if price is None or weight_grams is None or weight_grams <= 0:
        return None
    return round(price / weight_grams, 3)


def parse_in_stock(raw) -> bool:
    """Parse stock status from various formats."""
    if raw is None:
        return True  # Default to in-stock if not specified
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        text = raw.strip().lower()
        if text in ("true", "yes", "1", "in stock", "available", "in_stock"):
            return True
        if text in ("false", "no", "0", "out of stock", "sold out", "unavailable", "out_of_stock"):
            return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — Fingerprint & Deduplication
# ══════════════════════════════════════════════════════════════════════════════

def compute_fingerprint(
    product_name: str,
    brand: str,
    weight_grams: Optional[float],
    price: Optional[float],
) -> str:
    """
    Generate an MD5 fingerprint from the product's core identity fields.
    Two products with the same name, brand, weight, and price are considered
    duplicates even if they have different Dutchie IDs.
    """
    weight_str = f"{weight_grams:.4f}" if weight_grams is not None else "null"
    price_str = f"{price:.2f}" if price is not None else "null"
    raw_str = f"{product_name.lower()}|{brand.lower()}|{weight_str}|{price_str}"
    return hashlib.md5(raw_str.encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 5 — Raw Field Extractor
# ══════════════════════════════════════════════════════════════════════════════

def extract_raw_fields(raw: dict) -> dict:
    """
    Extract fields from a raw Dutchie GraphQL product record.
    Handles all known field-naming variants across Dutchie API versions:
      - Name / name / productName
      - brand (string or {name: ...} object)
      - thcContent / potencyThc.formatted / cannabinoids.thc.formatted / thc
      - Prices (list of objects) / price (flat) / options[].price
      - weight / options[].weight / netWeight
      - isSoldOut / inStock / status
    """
    # Name
    name = (
        raw.get("Name")
        or raw.get("name")
        or raw.get("productName")
        or raw.get("product_name")
        or ""
    )

    # Brand
    brand_raw = raw.get("brand", "")
    if isinstance(brand_raw, dict):
        brand = brand_raw.get("name", "")
    else:
        brand = str(brand_raw) if brand_raw else ""

    # Category
    category = (
        raw.get("category")
        or raw.get("Category")
        or raw.get("type")
        or ""
    )

    # Subcategory
    subcategory = raw.get("subcategory") or raw.get("Subcategory") or ""

    # Strain
    strain = (
        raw.get("strainType")
        or raw.get("strain_type")
        or raw.get("strain")
        or raw.get("Strain")
        or ""
    )

    # ── Cannabinoid extraction ─────────────────────────────────────────────
    # Dutchie API structure (confirmed from live API diagnostic, March 2026):
    #   THC → THCContent.range[0]  (e.g., [20.18])
    #   CBD → CBDContent.range[0]
    #   CBG/CBN/THCA/etc → cannabinoidsV2 list, each entry:
    #     { value: 0.1, unit: 'PERCENTAGE', cannabinoid: { name: 'CBN (Cannabinol)' } }
    
    def get_potency_content(raw_data, key_upper):
        """Extract from THCContent / CBDContent style fields."""
        content = raw_data.get(f"{key_upper}Content")
        if isinstance(content, dict):
            range_val = content.get("range")
            if isinstance(range_val, list) and range_val:
                return range_val[0]  # Take first value of range
            val = content.get("value")
            if val is not None:
                return val
        # Also try direct field (legacy)
        direct = raw_data.get(key_upper) or raw_data.get(key_upper.lower())
        if direct is not None:
            return direct
        return None

    def get_cannabinoids_v2(raw_data, search_key):
        """Extract from cannabinoidsV2 list by matching cannabinoid name."""
        canns_v2 = raw_data.get("cannabinoidsV2") or []
        search_lower = search_key.lower()
        for entry in canns_v2:
            if not isinstance(entry, dict):
                continue
            cann_obj = entry.get("cannabinoid", {})
            cann_name = (cann_obj.get("name") or "").lower()
            # Match by prefix: 'cbg' matches 'CBG (Cannabigerol)'
            if cann_name.startswith(search_lower):
                return entry.get("value")
        return None

    # THC: try THCContent first, then cannabinoidsV2 for THCA
    thc = get_potency_content(raw, "THC")
    if thc is None:
        thc = get_cannabinoids_v2(raw, "thca")  # THCA is the pre-cursor, most common
    if thc is None:
        thc = get_cannabinoids_v2(raw, "thc")
    
    # CBD
    cbd = get_potency_content(raw, "CBD")
    if cbd is None:
        cbd = get_cannabinoids_v2(raw, "cbd")
    
    # CBG
    cbg = get_cannabinoids_v2(raw, "cbg")
    
    # CBN
    cbn = get_cannabinoids_v2(raw, "cbn")

    # Price — try flat price, then Prices array, then options array
    price = raw.get("price")
    if price is None:
        prices_list = raw.get("Prices") or raw.get("prices")
        if isinstance(prices_list, list) and prices_list:
            first = prices_list[0]
            if isinstance(first, dict):
                price = first.get("price") or first.get("value")
            elif isinstance(first, (int, float)):
                price = first
    if price is None:
        options = raw.get("options") or []
        if isinstance(options, list):
            for opt in options:
                if isinstance(opt, dict) and opt.get("price") is not None:
                    price = opt["price"]
                    break

    # ── Weight extraction ───────────────────────────────────────────────
    # Dutchie API structure (confirmed from live API diagnostic, March 2026):
    #   raw.weight = 1000 (ALWAYS, this is a legacy/placeholder field - IGNORE IT)
    #   raw.rawOptions = ['7.0g']  <- USE THIS (human-readable weight string)
    #   raw.Options = ['1/4oz']    <- Also usable
    #   raw.measurements.netWeight.values[0] = 7000 (milligrams)
    weight = None
    
    # Strategy 1: rawOptions (most reliable - e.g. ['7.0g', '3.5g'])
    raw_opts = raw.get("rawOptions") or []
    if isinstance(raw_opts, list) and raw_opts:
        for opt in raw_opts:
            if opt and isinstance(opt, str) and re.search(r"[\d.]+\s*(g|mg|oz|lb)", opt, re.I):
                weight = opt
                break
    
    # Strategy 2: measurements.netWeight.values[0] (in milligrams)
    if weight is None:
        measurements = raw.get("measurements")
        if isinstance(measurements, dict):
            net_weight = measurements.get("netWeight")
            if isinstance(net_weight, dict):
                values = net_weight.get("values") or []
                unit = net_weight.get("unit", "").upper()
                if values and values[0]:
                    val = float(values[0])
                    if unit == "MILLIGRAMS":
                        weight = f"{val}mg"
                    elif unit == "GRAMS":
                        weight = f"{val}g"
                    else:
                        weight = f"{val}mg"  # Default assume mg
    
    # Strategy 3: Options list (e.g. ['1/4oz'])
    if weight is None:
        options_list = raw.get("Options") or raw.get("options") or []
        if isinstance(options_list, list) and options_list:
            for opt in options_list:
                if opt and isinstance(opt, str) and re.search(r"[\d./]+\s*(g|mg|oz|lb|eighth|quarter|half)", opt, re.I):
                    weight = opt
                    break
    
    # Strategy 4: Product name contains weight (e.g. "Blue Dream | Flower | 3.5g")
    if weight is None and name:
        # Look for weight at end of name after pipe separator
        pipe_match = re.search(r"\|\s*([\d.]+\s*(?:g|mg|oz|lb))\s*$", name, re.I)
        if pipe_match:
            weight = pipe_match.group(1)
        else:
            # General weight pattern anywhere in name
            name_weight_match = re.search(r"(\d+\.?\d*)\s*(g|mg|oz|lb|kg)", name, re.I)
            if name_weight_match:
                weight = name_weight_match.group(0)

    # Stock status
    in_stock = raw.get("inStock")
    if in_stock is None:
        is_sold_out = raw.get("isSoldOut")
        if is_sold_out is not None:
            in_stock = not is_sold_out
        else:
            status = raw.get("status", "")
            if isinstance(status, str) and status.lower() in ("sold out", "unavailable"):
                in_stock = False
            else:
                in_stock = True

    return {
        "id": raw.get("id") or raw.get("_id") or raw.get("product_id") or "",
        "name": name,
        "brand": brand,
        "category": category,
        "subcategory": subcategory,
        "strain": strain,
        "thc": thc,
        "cbd": cbd,
        "cbg": cbg,
        "cbn": cbn,
        "price": price,
        "weight": weight,
        "in_stock": in_stock,
    }


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 6 — GoldenSchemaPipeline Class
# ══════════════════════════════════════════════════════════════════════════════

class GoldenSchemaPipeline:
    """
    Orchestrates the full normalize → deduplicate → export pipeline.

    Usage:
        pipeline = GoldenSchemaPipeline(dispensary_slug="store-name")
        clean = pipeline.process(raw_items, source_url=url)
        await Actor.push_data(pipeline.to_dicts())
    """

    def __init__(self, dispensary_slug: str = ""):
        self.dispensary_slug = dispensary_slug
        self.products: list[GoldenProduct] = []
        self.stats = {
            "raw_count": 0,
            "clean_count": 0,
            "fingerprint_dupes": 0,
            "id_dupes": 0,
            "invalid_skipped": 0,
        }

    def process(
        self,
        raw_items: list[dict],
        source_url: str = "",
        scraped_at: str = "",
    ) -> list[GoldenProduct]:
        """Run the full pipeline on a list of raw Dutchie GraphQL records."""
        if not scraped_at:
            scraped_at = datetime.now(timezone.utc).isoformat()

        self.stats["raw_count"] = len(raw_items)
        seen_ids: set[str] = set()
        seen_fingerprints: set[str] = set()
        results: list[GoldenProduct] = []

        for raw in raw_items:
            fields = extract_raw_fields(raw)

            # Validation gate: skip records with no name AND no ID
            if not fields["name"] and not fields["id"]:
                self.stats["invalid_skipped"] += 1
                logger.warning(
                    f"Skipping record with no name and no ID: "
                    f"{json.dumps(raw, default=str)[:120]}..."
                )
                continue

            # Normalize every field
            product_name = normalize_name(fields["name"])
            brand = normalize_brand(fields["brand"])
            category, subcategory = normalize_category(fields["category"])
            # If raw subcategory is available and we didn't derive one, use it
            if not subcategory and fields["subcategory"]:
                subcategory = smart_title(str(fields["subcategory"]))
            strain_type = normalize_strain_type(fields["strain"])
            thc_pct = parse_percentage(fields["thc"])
            cbd_pct = parse_percentage(fields["cbd"])
            cbg_pct = parse_percentage(fields["cbg"])
            cbn_pct = parse_percentage(fields["cbn"])
            price = parse_price(fields["price"])
            weight_grams = parse_weight(fields["weight"])
            ppg = compute_price_per_gram(price, weight_grams)
            in_stock = parse_in_stock(fields["in_stock"])
            product_id = str(fields["id"]) if fields["id"] else ""

            # Dedup pass 1: fingerprint
            fp = compute_fingerprint(product_name, brand, weight_grams, price)
            if fp in seen_fingerprints:
                self.stats["fingerprint_dupes"] += 1
                continue
            seen_fingerprints.add(fp)

            # Dedup pass 2: product ID
            if product_id and product_id in seen_ids:
                self.stats["id_dupes"] += 1
                continue
            if product_id:
                seen_ids.add(product_id)

            product = GoldenProduct(
                product_id=product_id,
                product_name=product_name,
                brand=brand,
                category=category,
                subcategory=subcategory,
                strain_type=strain_type,
                thc_percentage=thc_pct,
                cbd_percentage=cbd_pct,
                cbg_percentage=cbg_pct,
                cbn_percentage=cbn_pct,
                weight_grams=weight_grams,
                price=price,
                price_per_gram=ppg,
                in_stock=in_stock,
                dispensary_slug=self.dispensary_slug,
                source_url=source_url,
                scraped_at=scraped_at,
                fingerprint=fp,
            )
            results.append(product)

        self.products = results
        self.stats["clean_count"] = len(results)

        logger.info(
            f"[{self.dispensary_slug}] Processed {self.stats['raw_count']} raw → "
            f"{self.stats['clean_count']} clean | "
            f"{self.stats['fingerprint_dupes']} fingerprint dupes | "
            f"{self.stats['id_dupes']} ID dupes | "
            f"{self.stats['invalid_skipped']} invalid/skipped"
        )

        return results

    def to_dicts(self) -> list[dict]:
        """Return all clean products as flat dicts (ready for Actor.push_data)."""
        return [p.to_dict() for p in self.products]

    def get_stats(self) -> dict:
        """Return pipeline statistics."""
        return dict(self.stats)

    # ── Export Helpers ─────────────────────────────────────────────────────

    def export_csv(self, filepath: str) -> None:
        """Write clean products to a CSV file."""
        if not self.products:
            logger.warning("No products to export.")
            return
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(SHEETS_HEADERS)
            for p in self.products:
                writer.writerow(p.to_row())
        logger.info(f"CSV exported → {filepath} ({len(self.products)} rows)")

    def export_csv_string(self) -> str:
        """Return clean products as a CSV string (for Key-Value Store)."""
        if not self.products:
            return ""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(SHEETS_HEADERS)
        for p in self.products:
            writer.writerow(p.to_row())
        return output.getvalue()

    def export_json(self, filepath: str) -> None:
        """Write clean products to a JSON file."""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.to_dicts(), f, indent=2, default=str)
        logger.info(f"JSON exported → {filepath} ({len(self.products)} rows)")
