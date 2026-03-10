"""
golden_schema.py — Dutchie Menu Scraper: Golden Schema v3.0
============================================================

Production-grade 77-field cannabis product schema for competitive intelligence,
analytics, Google Sheets, BI dashboards, and LLM/agent workflows.

Schema Sections:
  1. Source / Store Metadata    (fields 1–9)
  2. Product Identity           (fields 10–16)
  3. Product Classification     (fields 17–27)
  4. Size / Quantity            (fields 28–30)
  5. Potency / Labs             (fields 31–36)
  6. Terpenes                   (fields 37–48)
  7. Customer Experience        (fields 49–52)
  8. Pricing                    (fields 53–59)
  9. Availability / Fulfillment (fields 60–68)
  10. Merchandising / Badges    (fields 69–73)
  11. Scrape Metadata           (fields 74–77)

Parser version: 3.0
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("dutchie-scraper")

PARSER_VERSION = "3.0"
SCRAPER_ACTOR = "dutchie-menu-scraper"

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — Constants & Lookup Tables
# ══════════════════════════════════════════════════════════════════════════════

# Standard weight conversions
GRAMS_TO_OZ = 1 / 28.3495
OZ_TO_GRAMS = 28.3495

# Canonical size labels (grams → label)
SIZE_LABELS = {
    0.5:  "0.5g",
    1.0:  "1g",
    2.0:  "2g",
    2.5:  "2.5g",
    3.5:  "1/8 oz",
    5.0:  "5g",
    7.0:  "1/4 oz",
    10.0: "10g",
    14.0: "1/2 oz",
    28.0: "1 oz",
    56.0: "2 oz",
    100.0: "100g",
    112.0: "4 oz",
    448.0: "1 lb",
}

# Fraction-of-ounce text → grams
OZ_FRACTION_MAP = {
    "1/8": 3.5,
    "1/4": 7.0,
    "1/2": 14.0,
    "1/3": 9.33,
    "2/3": 18.67,
    "3/4": 21.0,
    "1":   28.0,
    "2":   56.0,
}

# Strain type normalization map
STRAIN_NORM_MAP = {
    "sativa":             "sativa",
    "sativa dominant":    "sativa-hybrid",
    "sativa-dominant":    "sativa-hybrid",
    "sativa hybrid":      "sativa-hybrid",
    "sativa-hybrid":      "sativa-hybrid",
    "mostly sativa":      "sativa-hybrid",
    "hybrid":             "hybrid",
    "balanced hybrid":    "hybrid",
    "balanced":           "hybrid",
    "hybrid/sativa":      "sativa-hybrid",
    "hybrid/indica":      "indica-hybrid",
    "indica hybrid":      "indica-hybrid",
    "indica-hybrid":      "indica-hybrid",
    "indica dominant":    "indica-hybrid",
    "indica-dominant":    "indica-hybrid",
    "mostly indica":      "indica-hybrid",
    "indica":             "indica",
    "cbd":                "cbd",
    "high cbd":           "cbd",
    "cbd dominant":       "cbd",
    "blend":              "blend",
    "mixed":              "blend",
}

# Known terpene name normalization
TERPENE_NORM_MAP = {
    "myrcene":         "myrcene",
    "β-myrcene":       "myrcene",
    "limonene":        "limonene",
    "d-limonene":      "limonene",
    "caryophyllene":   "caryophyllene",
    "β-caryophyllene": "caryophyllene",
    "pinene":          "pinene",
    "α-pinene":        "pinene",
    "β-pinene":        "pinene",
    "linalool":        "linalool",
    "humulene":        "humulene",
    "α-humulene":      "humulene",
    "terpinolene":     "terpinolene",
    "ocimene":         "ocimene",
    "bisabolol":       "bisabolol",
    "α-bisabolol":     "bisabolol",
    "nerolidol":       "nerolidol",
    "trans-nerolidol": "nerolidol",
}

# Known terpene field names in schema
TERPENE_FIELDS = [
    "myrcene", "limonene", "caryophyllene", "pinene",
    "linalool", "humulene", "terpinolene", "ocimene",
    "bisabolol", "nerolidol",
]

# Category normalization
CATEGORY_NORM_MAP = {
    "flower":        "Flower",
    "flowers":       "Flower",
    "pre-roll":      "Pre-Roll",
    "pre-rolls":     "Pre-Roll",
    "preroll":       "Pre-Roll",
    "prerolls":      "Pre-Roll",
    "vape":          "Vape",
    "vapes":         "Vape",
    "vaporizer":     "Vape",
    "vaporizers":    "Vape",
    "cartridge":     "Vape",
    "concentrate":   "Concentrate",
    "concentrates":  "Concentrate",
    "extract":       "Concentrate",
    "extracts":      "Concentrate",
    "edible":        "Edible",
    "edibles":       "Edible",
    "tincture":      "Tincture",
    "tinctures":     "Tincture",
    "topical":       "Topical",
    "topicals":      "Topical",
    "accessory":     "Accessory",
    "accessories":   "Accessory",
    "apparel":       "Apparel",
    "gear":          "Accessory",
}

# Unit type keywords for vapes/concentrates
UNIT_TYPE_KEYWORDS = {
    "510":        "510 Thread",
    "510 thread": "510 Thread",
    "aio":        "AIO",
    "all-in-one": "AIO",
    "all in one": "AIO",
    "disposable": "Disposable",
    "pax pod":    "PAX Pod",
    "pax":        "PAX Pod",
    "pod":        "Pod",
    "rosin":      "Rosin",
    "live rosin": "Live Rosin",
    "resin":      "Live Resin",
    "live resin": "Live Resin",
    "badder":     "Badder",
    "batter":     "Badder",
    "sauce":      "Sauce",
    "sugar":      "Sugar",
    "wax":        "Wax",
    "shatter":    "Shatter",
    "crumble":    "Crumble",
    "diamond":    "Diamonds",
    "diamonds":   "Diamonds",
    "hash":       "Hash",
    "bubble hash":"Bubble Hash",
    "kief":       "Kief",
    "distillate": "Distillate",
    "oil":        "Oil",
    "tincture":   "Tincture",
    "capsule":    "Capsule",
    "capsules":   "Capsule",
    "gummy":      "Gummy",
    "gummies":    "Gummy",
    "chocolate":  "Chocolate",
    "beverage":   "Beverage",
    "drink":      "Beverage",
    "blunt":      "Blunt",
    "dogwalker":  "Dogwalker",
    "infused":    "Infused Pre-Roll",
}

# Infusion type keywords
INFUSION_TYPE_KEYWORDS = {
    "kief infused":       "Kief Infused",
    "kief-infused":       "Kief Infused",
    "kief":               "Kief Infused",
    "rosin infused":      "Rosin Infused",
    "live rosin infused": "Live Rosin Infused",
    "resin infused":      "Live Resin Infused",
    "live resin infused": "Live Resin Infused",
    "distillate infused": "Distillate Infused",
    "hash infused":       "Hash Infused",
    "bubble hash infused":"Bubble Hash Infused",
    "wax infused":        "Wax Infused",
    "diamond infused":    "Diamond Infused",
    "diamonds infused":   "Diamond Infused",
    "oil infused":        "Oil Infused",
    "snowball":           "Kief Infused",
}

# Known effects tags
EFFECTS_KEYWORDS = {
    "energetic", "happy", "creative", "focused", "inspired",
    "uplifted", "euphoric", "giggly", "talkative", "aroused",
    "relaxed", "calm", "calming", "sleepy", "sedated",
    "couchlock", "tranquil", "hungry", "tingly",
    "pain relief", "stress relief", "anxiety relief",
}

# Known flavor/aroma keywords
FLAVOR_KEYWORDS = {
    "sweet", "earthy", "woody", "pine", "citrus", "lemon", "lime",
    "orange", "tropical", "fruity", "berry", "blueberry", "grape",
    "cherry", "mango", "pineapple", "melon", "peach", "apple",
    "diesel", "gassy", "fuel", "skunk", "pungent", "chemical",
    "spicy", "pepper", "herbal", "mint", "floral", "lavender",
    "vanilla", "chocolate", "coffee", "nutty", "creamy",
}

# Google Sheets / CSV column order
SHEETS_HEADERS = [
    # Source / Store Metadata
    "dispensary_id", "dispensary_slug", "dispensary_name",
    "dispensary_city", "dispensary_state",
    "source_platform", "source_category_url", "sort_order_context", "menu_position",
    # Product Identity
    "product_id", "product_name", "product_name_raw", "brand", "sku",
    "product_url", "image_url",
    # Product Classification
    "category", "subcategory", "product_form", "unit_type", "pack_count",
    "infusion_type", "strain_name", "strain_type_raw", "strain_type_normalized",
    "breeder", "lineage_cross",
    # Size / Quantity
    "size_label", "weight_grams", "weight_oz",
    # Potency / Labs
    "thc_percentage", "cbd_percentage", "tac_percentage",
    "thc_mg", "cbd_mg", "total_terpenes_percentage",
    # Terpenes
    "dominant_terpene", "terpene_profile",
    "myrcene_percentage", "limonene_percentage", "caryophyllene_percentage",
    "pinene_percentage", "linalool_percentage", "humulene_percentage",
    "terpinolene_percentage", "ocimene_percentage",
    "bisabolol_percentage", "nerolidol_percentage",
    # Customer Experience
    "reported_effects", "effects_tags", "flavor_notes", "aroma_notes",
    # Pricing
    "price", "price_original", "sale_price",
    "discount_amount", "discount_percent",
    "price_per_gram", "price_per_oz",
    # Availability / Fulfillment
    "in_stock", "stock_count", "low_stock_flag", "stock_status_raw",
    "pickup_available", "delivery_available",
    "medical_available", "recreational_available", "rec_or_med",
    # Merchandising / Badges
    "product_badges", "featured_flag", "staff_pick_flag", "new_flag", "sale_flag",
    # Scrape Metadata
    "scraped_at", "scrape_batch_id", "scraper_actor", "parser_version",
]


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — GoldenProduct Dataclass
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class GoldenProduct:
    """A fully normalized cannabis product record."""

    # Source / Store Metadata
    dispensary_id:          str = ""
    dispensary_slug:        str = ""
    dispensary_name:        str = ""
    dispensary_city:        Optional[str] = None
    dispensary_state:       Optional[str] = None
    source_platform:        str = "Dutchie"
    source_category_url:    Optional[str] = None
    sort_order_context:     Optional[str] = None
    menu_position:          Optional[int] = None

    # Product Identity
    product_id:             str = ""
    product_name:           str = ""
    product_name_raw:       str = ""
    brand:                  str = ""
    sku:                    Optional[str] = None
    product_url:            Optional[str] = None
    image_url:              Optional[str] = None

    # Product Classification
    category:               str = ""
    subcategory:            Optional[str] = None
    product_form:           Optional[str] = None
    unit_type:              Optional[str] = None
    pack_count:             Optional[int] = None
    infusion_type:          Optional[str] = None
    strain_name:            Optional[str] = None
    strain_type_raw:        Optional[str] = None
    strain_type_normalized: str = "unknown"
    breeder:                Optional[str] = None
    lineage_cross:          Optional[str] = None

    # Size / Quantity
    size_label:             Optional[str] = None
    weight_grams:           Optional[float] = None
    weight_oz:              Optional[float] = None

    # Potency / Labs
    thc_percentage:         Optional[float] = None
    cbd_percentage:         Optional[float] = None
    tac_percentage:         Optional[float] = None
    thc_mg:                 Optional[float] = None
    cbd_mg:                 Optional[float] = None
    total_terpenes_percentage: Optional[float] = None

    # Terpenes
    dominant_terpene:       Optional[str] = None
    terpene_profile:        Optional[str] = None
    myrcene_percentage:     Optional[float] = None
    limonene_percentage:    Optional[float] = None
    caryophyllene_percentage: Optional[float] = None
    pinene_percentage:      Optional[float] = None
    linalool_percentage:    Optional[float] = None
    humulene_percentage:    Optional[float] = None
    terpinolene_percentage: Optional[float] = None
    ocimene_percentage:     Optional[float] = None
    bisabolol_percentage:   Optional[float] = None
    nerolidol_percentage:   Optional[float] = None

    # Customer Experience
    reported_effects:       Optional[str] = None
    effects_tags:           Optional[str] = None
    flavor_notes:           Optional[str] = None
    aroma_notes:            Optional[str] = None

    # Pricing
    price:                  Optional[float] = None
    price_original:         Optional[float] = None
    sale_price:             Optional[float] = None
    discount_amount:        Optional[float] = None
    discount_percent:       Optional[float] = None
    price_per_gram:         Optional[float] = None
    price_per_oz:           Optional[float] = None

    # Availability / Fulfillment
    in_stock:               bool = True
    stock_count:            Optional[int] = None
    low_stock_flag:         bool = False
    stock_status_raw:       Optional[str] = None
    pickup_available:       Optional[bool] = None
    delivery_available:     Optional[bool] = None
    medical_available:      Optional[bool] = None
    recreational_available: Optional[bool] = None
    rec_or_med:             str = "recreational"

    # Merchandising / Badges
    product_badges:         Optional[str] = None
    featured_flag:          bool = False
    staff_pick_flag:        bool = False
    new_flag:               bool = False
    sale_flag:              bool = False

    # Scrape Metadata
    scraped_at:             str = ""
    scrape_batch_id:        str = ""
    scraper_actor:          str = SCRAPER_ACTOR
    parser_version:         str = PARSER_VERSION

    def to_dict(self) -> dict:
        """Return as a flat dict ordered by SHEETS_HEADERS."""
        d = asdict(self)
        return {k: d.get(k) for k in SHEETS_HEADERS}

    def to_row(self) -> list:
        """Return as a flat list ordered by SHEETS_HEADERS."""
        d = self.to_dict()
        return [d.get(k, "") for k in SHEETS_HEADERS]


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — Individual Field Parsers
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(str(val).replace("%", "").replace("$", "").strip())
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Safely convert a value to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _smart_title(s: str) -> str:
    """Title-case a string, handling hyphens and special words."""
    if not s:
        return s
    words = s.replace("-", " - ").split()
    return " ".join(w.capitalize() for w in words).replace(" - ", "-")


def parse_weight(raw_weight_str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Parse a weight string into (weight_grams, weight_oz, size_label).

    Handles:
    - Gram strings: "3.5g", "7.0g", "28g"
    - Ounce fractions: "1/8oz", "1/4 oz", "1oz"
    - Milligrams: "3500mg" -> 3.5g
    - Ounce decimals: "0.125oz" -> 3.5g
    - Returns (None, None, None) if unparseable.

    IMPORTANT: This function NEVER returns 1000g. If the input is the
    legacy Dutchie placeholder value of 1000, it returns None.
    """
    if raw_weight_str is None:
        return None, None, None

    s = str(raw_weight_str).strip().lower()

    # Reject the legacy Dutchie placeholder
    if s in ("1000", "1000.0", "1000g"):
        return None, None, None

    grams = None

    # Pattern 1: Fractional ounce (e.g., "1/8oz", "1/4 oz")
    frac_match = re.search(r"(\d+)\s*/\s*(\d+)\s*oz", s)
    if frac_match:
        numerator = float(frac_match.group(1))
        denominator = float(frac_match.group(2))
        if denominator != 0:
            oz = numerator / denominator
            grams = round(oz * OZ_TO_GRAMS, 4)

    # Pattern 2: Decimal ounce (e.g., "0.125oz", "1.0oz")
    if grams is None:
        oz_match = re.search(r"(\d+\.?\d*)\s*oz", s)
        if oz_match:
            oz = float(oz_match.group(1))
            grams = round(oz * OZ_TO_GRAMS, 4)

    # Pattern 3: Milligrams (e.g., "3500mg")
    if grams is None:
        mg_match = re.search(r"(\d+\.?\d*)\s*mg", s)
        if mg_match:
            mg = float(mg_match.group(1))
            # Only convert if it looks like a package weight (>= 500mg = 0.5g)
            # Smaller values are likely cannabinoid doses, not package weights
            if mg >= 500:
                grams = round(mg / 1000, 4)
            else:
                # This is a cannabinoid dose, not a package weight
                return None, None, None

    # Pattern 4: Grams (e.g., "3.5g", "7g", "28.0g")
    if grams is None:
        g_match = re.search(r"(\d+\.?\d*)\s*g\b", s)
        if g_match:
            grams = round(float(g_match.group(1)), 4)

    # Pattern 5: Plain number (assume grams if reasonable)
    if grams is None:
        plain_match = re.match(r"^(\d+\.?\d*)$", s)
        if plain_match:
            val = float(plain_match.group(1))
            if 0.1 <= val <= 500:  # Reasonable gram range
                grams = round(val, 4)

    if grams is None:
        return None, None, None

    # Snap to nearest known weight for clean values
    # Use a wider tolerance for ounce-derived values (e.g. 28.3495 → 28.0)
    for known_g in sorted(SIZE_LABELS.keys()):
        tolerance = 0.5 if known_g >= 14 else 0.15
        if abs(grams - known_g) < tolerance:
            grams = known_g
            break

    oz = round(grams * GRAMS_TO_OZ, 6)

    # Generate size_label
    size_label = SIZE_LABELS.get(grams)
    if size_label is None:
        if grams < 1:
            size_label = f"{grams}g"
        elif grams < 28:
            size_label = f"{grams}g"
        else:
            size_label = f"{grams}g"

    return grams, round(oz, 6), size_label


def parse_percentage(val) -> Optional[float]:
    """Parse a percentage value, returning None if absent or invalid."""
    if val is None:
        return None
    f = _safe_float(val)
    if f is None:
        return None
    # Sanity check: percentages should be 0-100
    if f < 0 or f > 100:
        return None
    return round(f, 2)


def parse_price(val) -> Optional[float]:
    """Parse a price value, returning None if absent or invalid."""
    if val is None:
        return None
    f = _safe_float(val)
    if f is None or f < 0:
        return None
    return round(f, 2)


def normalize_category(raw_cat: str) -> tuple[str, Optional[str]]:
    """Normalize a raw category string to a standard category and subcategory."""
    if not raw_cat:
        return "Other", None
    key = raw_cat.strip().lower()
    normalized = CATEGORY_NORM_MAP.get(key)
    if normalized:
        return normalized, None
    # Try prefix match
    for k, v in CATEGORY_NORM_MAP.items():
        if key.startswith(k):
            return v, None
    return _smart_title(raw_cat), None


def normalize_strain_type(raw: str) -> tuple[Optional[str], str]:
    """
    Normalize a raw strain type string.
    Returns (strain_type_raw, strain_type_normalized).
    """
    if not raw or str(raw).strip().lower() in ("none", "null", ""):
        return None, "unknown"
    raw_str = str(raw).strip()
    key = raw_str.lower()
    normalized = STRAIN_NORM_MAP.get(key)
    if normalized:
        return raw_str, normalized
    # Fuzzy match
    for k, v in STRAIN_NORM_MAP.items():
        if k in key:
            return raw_str, v
    return raw_str, "unknown"


def parse_strain_name(product_name: str, brand: str) -> Optional[str]:
    """
    Attempt to extract the strain name from the product name.
    Dutchie product names often follow: "Strain Name | Product Form | Size"
    """
    if not product_name:
        return None
    # Split by pipe separator
    parts = [p.strip() for p in product_name.split("|")]
    if len(parts) >= 2:
        # First part is usually the strain name
        candidate = parts[0].strip()
        # Remove brand name if it appears at the start
        if brand and candidate.lower().startswith(brand.lower()):
            candidate = candidate[len(brand):].strip(" -|")
        if candidate and len(candidate) > 1:
            return candidate
    return None


def parse_pack_count(product_name: str, raw: dict) -> Optional[int]:
    """
    Extract pack count from product name (e.g., "5 x 0.5g" -> 5)
    or from structured data.
    """
    # Check structured data first
    pack = raw.get("packCount") or raw.get("pack_count") or raw.get("quantity")
    if pack:
        return _safe_int(pack)

    # Parse from name: "14pk", "5 pack", "10-pack", "5 x 0.5g"
    if product_name:
        name_lower = product_name.lower()
        patterns = [
            r"(\d+)\s*pk\b",
            r"(\d+)\s*pack\b",
            r"(\d+)-pack\b",
            r"(\d+)\s*x\s*[\d.]+\s*g",
            r"(\d+)\s*count\b",
            r"(\d+)\s*ct\b",
            r"(\d+)\s*piece",
        ]
        for pat in patterns:
            m = re.search(pat, name_lower)
            if m:
                count = _safe_int(m.group(1))
                if count and 1 < count <= 1000:
                    return count
    return None


def parse_unit_type(product_name: str, category: str, subcategory: str) -> Optional[str]:
    """Detect the unit type from product name and category."""
    if not product_name:
        return None
    name_lower = product_name.lower()
    for keyword, unit in UNIT_TYPE_KEYWORDS.items():
        if keyword in name_lower:
            return unit
    return None


def parse_infusion_type(product_name: str) -> Optional[str]:
    """Detect the infusion type from the product name."""
    if not product_name:
        return None
    name_lower = product_name.lower()
    for keyword, infusion in INFUSION_TYPE_KEYWORDS.items():
        if keyword in name_lower:
            return infusion
    return None


def parse_terpenes(raw: dict) -> dict:
    """
    Extract terpene data from a raw Dutchie product record.
    Returns a dict with terpene field values.
    """
    result = {
        "total_terpenes_percentage": None,
        "dominant_terpene": None,
        "terpene_profile": None,
    }
    for t in TERPENE_FIELDS:
        result[f"{t}_percentage"] = None

    # Try structured terpenes list
    terpenes_raw = (
        raw.get("terpenes")
        or raw.get("Terpenes")
        or raw.get("dominantTerpenes")
        or []
    )

    terpene_values = {}

    if isinstance(terpenes_raw, list):
        for entry in terpenes_raw:
            if not isinstance(entry, dict):
                continue
            name = (
                entry.get("name")
                or entry.get("terpene", {}).get("name", "")
                or ""
            ).lower().strip()
            value = _safe_float(
                entry.get("value")
                or entry.get("percentage")
                or entry.get("amount")
            )
            norm_name = TERPENE_NORM_MAP.get(name)
            if norm_name and value is not None:
                terpene_values[norm_name] = value

    elif isinstance(terpenes_raw, dict):
        for name, value in terpenes_raw.items():
            norm_name = TERPENE_NORM_MAP.get(name.lower())
            if norm_name:
                terpene_values[norm_name] = _safe_float(value)

    # Total terpenes
    total_terp = _safe_float(
        raw.get("totalTerpenes")
        or raw.get("total_terpenes")
        or raw.get("terpeneTotal")
    )
    if total_terp is None and terpene_values:
        total_terp = round(sum(v for v in terpene_values.values() if v), 4)
    result["total_terpenes_percentage"] = total_terp

    # Dominant terpene
    if terpene_values:
        dominant = max(terpene_values, key=lambda k: terpene_values[k] or 0)
        result["dominant_terpene"] = _smart_title(dominant)
        result["terpene_profile"] = ", ".join(
            _smart_title(t) for t in sorted(terpene_values, key=lambda k: terpene_values[k] or 0, reverse=True)
        )
        for t in TERPENE_FIELDS:
            result[f"{t}_percentage"] = terpene_values.get(t)

    return result


def parse_effects_and_flavors(raw: dict) -> dict:
    """
    Extract reported effects, effects tags, flavor notes, and aroma notes.
    """
    result = {
        "reported_effects": None,
        "effects_tags": None,
        "flavor_notes": None,
        "aroma_notes": None,
    }

    # Structured effects from Dutchie API
    effects_raw = raw.get("effects") or {}
    if isinstance(effects_raw, dict):
        # Dutchie returns effects as {effect_name: score}
        sorted_effects = sorted(effects_raw.items(), key=lambda x: x[1] or 0, reverse=True)
        top_effects = [_smart_title(k) for k, v in sorted_effects if v and v > 0]
        if top_effects:
            result["reported_effects"] = ", ".join(top_effects[:5])
            result["effects_tags"] = ", ".join(
                e.lower() for e in top_effects[:5]
                if e.lower() in EFFECTS_KEYWORDS
            ) or None

    elif isinstance(effects_raw, list):
        top_effects = [_smart_title(str(e)) for e in effects_raw if e]
        if top_effects:
            result["reported_effects"] = ", ".join(top_effects[:5])

    # Flavor / aroma from structured fields
    flavors_raw = raw.get("flavors") or raw.get("Flavors") or []
    if isinstance(flavors_raw, list) and flavors_raw:
        result["flavor_notes"] = ", ".join(
            _smart_title(str(f)) for f in flavors_raw if f
        )

    aromas_raw = raw.get("aromas") or raw.get("Aromas") or []
    if isinstance(aromas_raw, list) and aromas_raw:
        result["aroma_notes"] = ", ".join(
            _smart_title(str(a)) for a in aromas_raw if a
        )

    return result


def parse_stock_status(raw: dict) -> dict:
    """
    Extract stock status information.
    """
    result = {
        "in_stock": True,
        "stock_count": None,
        "low_stock_flag": False,
        "stock_status_raw": None,
    }

    # Direct status field
    status = raw.get("Status") or raw.get("status") or ""
    if status:
        result["stock_status_raw"] = str(status)
        if str(status).lower() in ("inactive", "out of stock", "outofstock"):
            result["in_stock"] = False

    # inStock boolean
    in_stock = raw.get("inStock")
    if in_stock is not None:
        result["in_stock"] = bool(in_stock)

    # isBelowThreshold signals low stock
    below_threshold = raw.get("isBelowThreshold") or raw.get("isBelowKioskThreshold")
    if below_threshold:
        result["low_stock_flag"] = True

    # Inventory count
    inventory = raw.get("inventory") or raw.get("quantity") or raw.get("stockCount")
    if inventory is not None:
        count = _safe_int(inventory)
        if count is not None:
            result["stock_count"] = count
            if count <= 5:
                result["low_stock_flag"] = True
                result["stock_status_raw"] = f"Only {count} left"

    return result


def parse_pricing(raw: dict, weight_grams: Optional[float]) -> dict:
    """
    Extract and compute all pricing fields.
    """
    result = {
        "price": None,
        "price_original": None,
        "sale_price": None,
        "discount_amount": None,
        "discount_percent": None,
        "price_per_gram": None,
        "price_per_oz": None,
        "sale_flag": False,
    }

    # Current (final) price
    price = None
    for field_name in ("price", "Price", "recPrice"):
        val = raw.get(field_name)
        if val is not None:
            price = parse_price(val)
            if price is not None:
                break

    if price is None:
        prices = raw.get("Prices") or raw.get("recPrices") or raw.get("medicalPrices") or []
        if isinstance(prices, list) and prices:
            price = parse_price(prices[0])

    if price is None:
        options = raw.get("Options") or []
        if isinstance(options, list):
            for opt in options:
                if isinstance(opt, dict):
                    p = parse_price(opt.get("price") or opt.get("Price"))
                    if p is not None:
                        price = p
                        break

    result["price"] = price

    # Special / sale price
    special_prices = (
        raw.get("specialPrices")
        or raw.get("recSpecialPrices")
        or raw.get("medicalSpecialPrices")
        or []
    )
    if isinstance(special_prices, list) and special_prices:
        sale_price = parse_price(special_prices[0])
        if sale_price is not None and price is not None and sale_price < price:
            result["sale_price"] = sale_price
            result["price_original"] = price
            result["price"] = sale_price
            result["sale_flag"] = True
            result["discount_amount"] = round(price - sale_price, 2)
            result["discount_percent"] = round((price - sale_price) / price * 100, 1)

    # Compare-at price
    compare_at = raw.get("compareAtPrice") or raw.get("originalPrice")
    if compare_at is not None:
        orig = parse_price(compare_at)
        if orig is not None and result["price"] is not None and orig > result["price"]:
            result["price_original"] = orig
            result["sale_flag"] = True
            if result["discount_amount"] is None:
                result["discount_amount"] = round(orig - result["price"], 2)
                result["discount_percent"] = round((orig - result["price"]) / orig * 100, 1)

    # Computed price-per-gram and price-per-oz
    if result["price"] is not None and weight_grams and weight_grams > 0:
        result["price_per_gram"] = round(result["price"] / weight_grams, 4)
        result["price_per_oz"] = round(result["price"] / (weight_grams * GRAMS_TO_OZ), 4)

    return result


def parse_cannabinoids(raw: dict, category: str) -> dict:
    """
    Extract all cannabinoid fields from a raw Dutchie product record.
    Handles both THCContent/CBDContent and cannabinoidsV2 structures.
    """
    result = {
        "thc_percentage": None,
        "cbd_percentage": None,
        "tac_percentage": None,
        "thc_mg": None,
        "cbd_mg": None,
        "cbg_percentage": None,
        "cbn_percentage": None,
    }

    def get_potency_content(key_upper):
        """Extract from THCContent / CBDContent style fields."""
        content = raw.get(f"{key_upper}Content")
        if isinstance(content, dict):
            range_val = content.get("range")
            if isinstance(range_val, list) and range_val:
                return _safe_float(range_val[0])
            val = content.get("value")
            if val is not None:
                return _safe_float(val)
        direct = raw.get(key_upper) or raw.get(key_upper.lower())
        if direct is not None:
            return _safe_float(direct)
        return None

    def get_cannabinoids_v2(search_key):
        """Extract from cannabinoidsV2 list by matching cannabinoid name prefix."""
        canns_v2 = raw.get("cannabinoidsV2") or []
        search_lower = search_key.lower()
        for entry in canns_v2:
            if not isinstance(entry, dict):
                continue
            cann_obj = entry.get("cannabinoid", {})
            cann_name = (cann_obj.get("name") or "").lower()
            if cann_name.startswith(search_lower):
                return _safe_float(entry.get("value"))
        return None

    # THC: prefer THCContent (which includes THCA range), fallback to cannabinoidsV2
    thc = get_potency_content("THC")
    if thc is None:
        thc = get_cannabinoids_v2("thca")
    if thc is None:
        thc = get_cannabinoids_v2("thc")
    result["thc_percentage"] = parse_percentage(thc)

    # CBD
    cbd = get_potency_content("CBD")
    if cbd is None:
        cbd = get_cannabinoids_v2("cbd")
    result["cbd_percentage"] = parse_percentage(cbd)

    # CBG
    cbg = get_cannabinoids_v2("cbg")
    result["cbg_percentage"] = parse_percentage(cbg)

    # CBN
    cbn = get_cannabinoids_v2("cbn")
    result["cbn_percentage"] = parse_percentage(cbn)

    # TAC (Total Active Cannabinoids)
    tac = _safe_float(raw.get("tac") or raw.get("TAC") or raw.get("totalActiveCannabinoids"))
    if tac is None:
        # Sum known cannabinoids as an approximation
        known = [v for v in [result["thc_percentage"], result["cbd_percentage"],
                              result["cbg_percentage"], result["cbn_percentage"]]
                 if v is not None]
        if known:
            tac = round(sum(known), 2)
    result["tac_percentage"] = parse_percentage(tac)

    # mg-based dosing (for edibles, tinctures, topicals, capsules, beverages)
    dose_categories = {"edible", "tincture", "topical", "capsule", "beverage"}
    if category.lower() in dose_categories:
        # THC mg
        thc_mg_val = (
            raw.get("thcMg") or raw.get("thc_mg")
            or raw.get("thcDosage") or raw.get("thcDose")
        )
        if thc_mg_val is None:
            # Try to extract from cannabinoidsV2 where unit is MILLIGRAMS
            for entry in (raw.get("cannabinoidsV2") or []):
                if not isinstance(entry, dict):
                    continue
                cann_name = (entry.get("cannabinoid", {}).get("name") or "").lower()
                unit = (entry.get("unit") or "").upper()
                if cann_name.startswith("thc") and unit == "MILLIGRAMS":
                    thc_mg_val = entry.get("value")
                    break
        result["thc_mg"] = _safe_float(thc_mg_val)

        # CBD mg
        cbd_mg_val = (
            raw.get("cbdMg") or raw.get("cbd_mg")
            or raw.get("cbdDosage") or raw.get("cbdDose")
        )
        if cbd_mg_val is None:
            for entry in (raw.get("cannabinoidsV2") or []):
                if not isinstance(entry, dict):
                    continue
                cann_name = (entry.get("cannabinoid", {}).get("name") or "").lower()
                unit = (entry.get("unit") or "").upper()
                if cann_name.startswith("cbd") and unit == "MILLIGRAMS":
                    cbd_mg_val = entry.get("value")
                    break
        result["cbd_mg"] = _safe_float(cbd_mg_val)

    return result


def parse_badges(raw: dict) -> dict:
    """Extract merchandising badges and flags."""
    result = {
        "product_badges": None,
        "featured_flag": False,
        "staff_pick_flag": False,
        "new_flag": False,
        "sale_flag": False,
    }

    badges = []

    featured = raw.get("featured") or raw.get("isFeatured")
    if featured:
        result["featured_flag"] = True
        badges.append("Featured")

    staff_pick = raw.get("staffPick") or raw.get("isStaffPick") or raw.get("staff_pick")
    if staff_pick:
        result["staff_pick_flag"] = True
        badges.append("Staff Pick")

    # Check for "new" badge
    badge_raw = raw.get("collectionCardBadge") or raw.get("badge") or ""
    if badge_raw:
        badge_str = str(badge_raw).lower()
        if "new" in badge_str:
            result["new_flag"] = True
            badges.append("New")
        if "sale" in badge_str or "special" in badge_str:
            result["sale_flag"] = True
            badges.append("Sale")
        if "staff" in badge_str:
            result["staff_pick_flag"] = True
            badges.append("Staff Pick")

    # Check special data
    special = raw.get("special") or raw.get("specialData")
    if special:
        result["sale_flag"] = True
        if "Sale" not in badges:
            badges.append("Sale")

    if badges:
        result["product_badges"] = ", ".join(dict.fromkeys(badges))  # deduplicate

    return result


def parse_fulfillment(raw: dict, pricing_type: str = "rec") -> dict:
    """Extract fulfillment and availability context."""
    result = {
        "pickup_available": None,
        "delivery_available": None,
        "medical_available": None,
        "recreational_available": None,
        "rec_or_med": pricing_type,
    }

    # Pickup / delivery
    result["pickup_available"] = bool(raw.get("pickupAvailable", True))
    result["delivery_available"] = bool(raw.get("deliveryAvailable", False))

    # Medical / recreational
    med_only = raw.get("medicalOnly") or raw.get("isMedicalOnly")
    rec_only = raw.get("recOnly") or raw.get("isRecOnly")

    if med_only:
        result["medical_available"] = True
        result["recreational_available"] = False
        result["rec_or_med"] = "medical"
    elif rec_only:
        result["medical_available"] = False
        result["recreational_available"] = True
        result["rec_or_med"] = "recreational"
    else:
        # Check if medical prices exist
        med_prices = raw.get("medicalPrices") or []
        rec_prices = raw.get("recPrices") or []
        result["medical_available"] = bool(med_prices)
        result["recreational_available"] = bool(rec_prices) or True

    return result


def extract_weight_from_raw(raw: dict, product_name: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Multi-strategy weight extraction from a Dutchie raw product record.
    Returns (weight_grams, weight_oz, size_label).
    """
    # Strategy 1: rawOptions (most reliable - e.g. ['7.0g', '3.5g'])
    raw_opts = raw.get("rawOptions") or []
    if isinstance(raw_opts, list):
        for opt in raw_opts:
            if opt and isinstance(opt, str):
                g, oz, label = parse_weight(opt)
                if g is not None:
                    return g, oz, label

    # Strategy 2: measurements.netWeight.values[0] (in milligrams)
    measurements = raw.get("measurements")
    if isinstance(measurements, dict):
        net_weight = measurements.get("netWeight")
        if isinstance(net_weight, dict):
            values = net_weight.get("values") or []
            unit = (net_weight.get("unit") or "").upper()
            if values and values[0]:
                val = float(values[0])
                if unit == "MILLIGRAMS":
                    weight_str = f"{val}mg"
                elif unit == "GRAMS":
                    weight_str = f"{val}g"
                elif unit == "OUNCES":
                    weight_str = f"{val}oz"
                else:
                    weight_str = f"{val}mg"
                g, oz, label = parse_weight(weight_str)
                if g is not None:
                    return g, oz, label

    # Strategy 3: Options list (e.g. ['1/4oz'])
    options_list = raw.get("Options") or raw.get("options") or []
    if isinstance(options_list, list):
        for opt in options_list:
            if opt and isinstance(opt, str):
                g, oz, label = parse_weight(opt)
                if g is not None:
                    return g, oz, label

    # Strategy 4: Product name contains weight (e.g. "Blue Dream | Flower | 3.5g")
    if product_name:
        # Look for weight at end of name after pipe separator
        pipe_match = re.search(r"\|\s*([\d./]+\s*(?:g|mg|oz|lb))\s*$", product_name, re.I)
        if pipe_match:
            g, oz, label = parse_weight(pipe_match.group(1))
            if g is not None:
                return g, oz, label
        # General weight pattern anywhere in name
        name_weight_match = re.search(r"(\d+\.?\d*)\s*(g|mg|oz|lb|kg)\b", product_name, re.I)
        if name_weight_match:
            g, oz, label = parse_weight(name_weight_match.group(0))
            if g is not None:
                return g, oz, label

    return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — Validation
# ══════════════════════════════════════════════════════════════════════════════

def validate_product(product: GoldenProduct) -> list[str]:
    """
    Run validation checks on a GoldenProduct.
    Returns a list of warning strings (empty list = valid).
    """
    warnings = []

    # Weight sanity check
    if product.weight_grams is not None:
        if product.weight_grams > 500:
            warnings.append(
                f"weight_grams={product.weight_grams} is implausibly large "
                f"(possible mg-to-g confusion) for '{product.product_name}'"
            )
        if product.weight_grams <= 0:
            warnings.append(f"weight_grams={product.weight_grams} is zero or negative")

    # Strain type enum check
    valid_strain_types = {
        "sativa", "sativa-hybrid", "hybrid", "indica-hybrid",
        "indica", "cbd", "blend", "unknown"
    }
    if product.strain_type_normalized not in valid_strain_types:
        warnings.append(
            f"strain_type_normalized='{product.strain_type_normalized}' "
            f"is not in the allowed enum"
        )

    # Terpene and TAC percentage sanity
    for field_name in ["total_terpenes_percentage", "tac_percentage",
                       "thc_percentage", "cbd_percentage"]:
        val = getattr(product, field_name, None)
        if val is not None and (val < 0 or val > 100):
            warnings.append(f"{field_name}={val} is outside 0-100 range")

    # Price sanity
    if product.price is not None and not isinstance(product.price, (int, float)):
        warnings.append(f"price='{product.price}' is not numeric")

    if (product.sale_price is not None and product.price_original is not None
            and product.sale_price > product.price_original):
        warnings.append(
            f"sale_price={product.sale_price} exceeds "
            f"price_original={product.price_original}"
        )

    # Pack count
    if product.pack_count is not None and not isinstance(product.pack_count, int):
        warnings.append(f"pack_count='{product.pack_count}' is not an integer")

    # Boolean fields
    for bool_field in ["in_stock", "low_stock_flag", "featured_flag",
                       "staff_pick_flag", "new_flag", "sale_flag"]:
        val = getattr(product, bool_field, None)
        if val is not None and not isinstance(val, bool):
            warnings.append(f"{bool_field}='{val}' is not a boolean")

    return warnings


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 5 — GoldenSchemaPipeline
# ══════════════════════════════════════════════════════════════════════════════

class GoldenSchemaPipeline:
    """
    Orchestrates the full extract → normalize → validate → deduplicate → export
    pipeline for Dutchie product data.

    Usage:
        pipeline = GoldenSchemaPipeline(
            dispensary_id="abc123",
            dispensary_slug="my-store",
            dispensary_name="My Store",
            pricing_type="rec",
        )
        clean = pipeline.process(raw_items, source_url=url)
        await Actor.push_data(pipeline.to_dicts())
    """

    def __init__(
        self,
        dispensary_id: str = "",
        dispensary_slug: str = "",
        dispensary_name: str = "",
        dispensary_city: Optional[str] = None,
        dispensary_state: Optional[str] = None,
        pricing_type: str = "rec",
        scrape_batch_id: Optional[str] = None,
    ):
        self.dispensary_id = dispensary_id
        self.dispensary_slug = dispensary_slug
        self.dispensary_name = dispensary_name
        self.dispensary_city = dispensary_city
        self.dispensary_state = dispensary_state
        self.pricing_type = pricing_type
        self.scrape_batch_id = scrape_batch_id or str(uuid.uuid4())[:8]
        self.products: list[GoldenProduct] = []
        self.stats = {
            "raw_count": 0,
            "clean_count": 0,
            "id_dupes": 0,
            "invalid_skipped": 0,
            "validation_warnings": 0,
        }

    def process(
        self,
        raw_items: list[dict],
        source_url: str = "",
        scraped_at: str = "",
        sort_order_context: str = "popularSortIdx",
    ) -> list[GoldenProduct]:
        """Run the full pipeline on a list of raw Dutchie GraphQL records."""
        if not scraped_at:
            scraped_at = datetime.now(timezone.utc).isoformat()

        self.stats["raw_count"] = len(raw_items)
        seen_ids: set[str] = set()
        results: list[GoldenProduct] = []

        for position, raw in enumerate(raw_items, start=1):
            product_id = str(raw.get("id") or raw.get("_id") or "")
            product_name_raw = str(
                raw.get("Name") or raw.get("name") or raw.get("productName") or ""
            ).strip()

            # Validation gate: skip records with no name AND no ID
            if not product_name_raw and not product_id:
                self.stats["invalid_skipped"] += 1
                continue

            # Dedup by product ID
            if product_id and product_id in seen_ids:
                self.stats["id_dupes"] += 1
                continue
            if product_id:
                seen_ids.add(product_id)

            # ── Brand ──────────────────────────────────────────────────────
            brand_raw = raw.get("brand") or raw.get("Brand") or {}
            if isinstance(brand_raw, dict):
                brand = str(brand_raw.get("name") or brand_raw.get("Name") or "").strip()
            else:
                brand = str(brand_raw).strip()
            brand_name = raw.get("brandName") or raw.get("BrandName") or ""
            if not brand and brand_name:
                brand = str(brand_name).strip()

            # ── Category ──────────────────────────────────────────────────
            cat_raw = (
                raw.get("type") or raw.get("Type")
                or raw.get("category") or raw.get("Category")
                or ""
            )
            category, _ = normalize_category(str(cat_raw))

            subcat_raw = (
                raw.get("subcategory") or raw.get("Subcategory")
                or raw.get("subType") or raw.get("subtype")
                or ""
            )
            subcategory = _smart_title(str(subcat_raw)) if subcat_raw else None

            # ── Strain ────────────────────────────────────────────────────
            strain_raw = (
                raw.get("strainType") or raw.get("StrainType")
                or raw.get("strain_type") or raw.get("strain")
                or raw.get("Strain") or ""
            )
            strain_type_raw, strain_type_normalized = normalize_strain_type(str(strain_raw))
            strain_name = parse_strain_name(product_name_raw, brand)

            # ── Product Name (cleaned) ────────────────────────────────────
            # Remove size suffix from name for cleaner display
            product_name_clean = re.sub(
                r"\s*\|\s*[\d./]+\s*(?:g|mg|oz|lb)\s*$",
                "",
                product_name_raw,
                flags=re.I,
            ).strip()
            # Also remove trailing pipe segments that are just the form
            product_name_clean = re.sub(r"\s*\|\s*[^|]+$", "", product_name_clean).strip()
            if not product_name_clean:
                product_name_clean = product_name_raw

            # ── Weight ────────────────────────────────────────────────────
            weight_grams, weight_oz, size_label = extract_weight_from_raw(raw, product_name_raw)

            # ── Cannabinoids ──────────────────────────────────────────────
            cann = parse_cannabinoids(raw, category)

            # ── Terpenes ──────────────────────────────────────────────────
            terp = parse_terpenes(raw)

            # ── Effects & Flavors ─────────────────────────────────────────
            exp = parse_effects_and_flavors(raw)

            # ── Pricing ───────────────────────────────────────────────────
            pricing = parse_pricing(raw, weight_grams)

            # ── Stock Status ──────────────────────────────────────────────
            stock = parse_stock_status(raw)

            # ── Fulfillment ───────────────────────────────────────────────
            fulfillment = parse_fulfillment(raw, self.pricing_type)

            # ── Badges ────────────────────────────────────────────────────
            badges = parse_badges(raw)
            # Merge sale_flag from pricing
            if pricing.get("sale_flag"):
                badges["sale_flag"] = True
                if badges["product_badges"]:
                    if "Sale" not in badges["product_badges"]:
                        badges["product_badges"] += ", Sale"
                else:
                    badges["product_badges"] = "Sale"

            # ── Classification extras ─────────────────────────────────────
            unit_type = parse_unit_type(product_name_raw, category, subcategory or "")
            pack_count = parse_pack_count(product_name_raw, raw)
            infusion_type = parse_infusion_type(product_name_raw)

            # ── URLs & Images ─────────────────────────────────────────────
            cname = raw.get("cName") or self.dispensary_slug
            product_url = (
                f"https://dutchie.com/dispensary/{self.dispensary_slug}"
                f"/product/{cname}"
                if cname else source_url
            )
            image_url = (
                raw.get("Image") or raw.get("image")
                or (raw.get("images") or [{}])[0].get("url")
                if isinstance(raw.get("images"), list) and raw.get("images")
                else None
            )

            # ── Build GoldenProduct ───────────────────────────────────────
            product = GoldenProduct(
                # Source / Store Metadata
                dispensary_id=self.dispensary_id,
                dispensary_slug=self.dispensary_slug,
                dispensary_name=self.dispensary_name,
                dispensary_city=self.dispensary_city,
                dispensary_state=self.dispensary_state,
                source_platform="Dutchie",
                source_category_url=source_url or None,
                sort_order_context=sort_order_context,
                menu_position=position,
                # Product Identity
                product_id=product_id,
                product_name=product_name_clean,
                product_name_raw=product_name_raw,
                brand=brand,
                sku=raw.get("sku") or raw.get("SKU") or None,
                product_url=product_url,
                image_url=image_url,
                # Product Classification
                category=category,
                subcategory=subcategory,
                product_form=None,  # Derived from subcategory/unit_type if needed
                unit_type=unit_type,
                pack_count=pack_count,
                infusion_type=infusion_type,
                strain_name=strain_name,
                strain_type_raw=strain_type_raw,
                strain_type_normalized=strain_type_normalized,
                breeder=raw.get("breeder") or raw.get("Breeder") or None,
                lineage_cross=raw.get("lineage") or raw.get("Lineage") or raw.get("genetics") or None,
                # Size / Quantity
                size_label=size_label,
                weight_grams=weight_grams,
                weight_oz=weight_oz,
                # Potency / Labs
                thc_percentage=cann["thc_percentage"],
                cbd_percentage=cann["cbd_percentage"],
                tac_percentage=cann["tac_percentage"],
                thc_mg=cann["thc_mg"],
                cbd_mg=cann["cbd_mg"],
                total_terpenes_percentage=terp["total_terpenes_percentage"],
                # Terpenes
                dominant_terpene=terp["dominant_terpene"],
                terpene_profile=terp["terpene_profile"],
                myrcene_percentage=terp.get("myrcene_percentage"),
                limonene_percentage=terp.get("limonene_percentage"),
                caryophyllene_percentage=terp.get("caryophyllene_percentage"),
                pinene_percentage=terp.get("pinene_percentage"),
                linalool_percentage=terp.get("linalool_percentage"),
                humulene_percentage=terp.get("humulene_percentage"),
                terpinolene_percentage=terp.get("terpinolene_percentage"),
                ocimene_percentage=terp.get("ocimene_percentage"),
                bisabolol_percentage=terp.get("bisabolol_percentage"),
                nerolidol_percentage=terp.get("nerolidol_percentage"),
                # Customer Experience
                reported_effects=exp["reported_effects"],
                effects_tags=exp["effects_tags"],
                flavor_notes=exp["flavor_notes"],
                aroma_notes=exp["aroma_notes"],
                # Pricing
                price=pricing["price"],
                price_original=pricing["price_original"],
                sale_price=pricing["sale_price"],
                discount_amount=pricing["discount_amount"],
                discount_percent=pricing["discount_percent"],
                price_per_gram=pricing["price_per_gram"],
                price_per_oz=pricing["price_per_oz"],
                # Availability / Fulfillment
                in_stock=stock["in_stock"],
                stock_count=stock["stock_count"],
                low_stock_flag=stock["low_stock_flag"],
                stock_status_raw=stock["stock_status_raw"],
                pickup_available=fulfillment["pickup_available"],
                delivery_available=fulfillment["delivery_available"],
                medical_available=fulfillment["medical_available"],
                recreational_available=fulfillment["recreational_available"],
                rec_or_med=fulfillment["rec_or_med"],
                # Merchandising / Badges
                product_badges=badges["product_badges"],
                featured_flag=badges["featured_flag"],
                staff_pick_flag=badges["staff_pick_flag"],
                new_flag=badges["new_flag"],
                sale_flag=badges["sale_flag"],
                # Scrape Metadata
                scraped_at=scraped_at,
                scrape_batch_id=self.scrape_batch_id,
                scraper_actor=SCRAPER_ACTOR,
                parser_version=PARSER_VERSION,
            )

            # ── Validation ────────────────────────────────────────────────
            warnings = validate_product(product)
            if warnings:
                self.stats["validation_warnings"] += len(warnings)
                for w in warnings:
                    logger.warning(f"[{self.dispensary_slug}] Validation: {w}")

            results.append(product)

        self.products = results
        self.stats["clean_count"] = len(results)

        logger.info(
            f"[{self.dispensary_slug}] Processed {self.stats['raw_count']} raw → "
            f"{self.stats['clean_count']} clean | "
            f"{self.stats['id_dupes']} ID dupes | "
            f"{self.stats['invalid_skipped']} invalid/skipped | "
            f"{self.stats['validation_warnings']} validation warnings"
        )

        return results

    def to_dicts(self) -> list[dict]:
        """Return all clean products as flat dicts (ready for Actor.push_data)."""
        return [p.to_dict() for p in self.products]

    def get_stats(self) -> dict:
        """Return pipeline statistics."""
        return dict(self.stats)

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
