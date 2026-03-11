import pytest
from src.main import normalize_product, extract_dispensary_name


class TestNormalizeProduct:
    def test_full_product(self):
        raw = {
            "id": "abc123",
            "Name": "Blue Dream",
            "brand": {"name": "Top Shelf"},
            "category": "Flower",
            "subcategory": "Hybrid",
            "strainType": "hybrid",
            "Prices": [29.99, 49.99],
            "thc": "22%",
            "cbd": "0.5%",
            "isSoldOut": False,
        }
        result = normalize_product(raw, "Test Dispensary")

        assert result["dispensary_name"] == "Test Dispensary"
        assert result["product_id"] == "abc123"
        assert result["name"] == "Blue Dream"
        assert result["brand"] == "Top Shelf"
        assert result["category"] == "Flower"
        assert result["subcategory"] == "Hybrid"
        assert result["strain_type"] == "hybrid"
        assert result["prices"] == [29.99, 49.99]
        assert result["thc"] == "22%"
        assert result["cbd"] == "0.5%"
        assert result["is_sold_out"] is False
        assert "scraped_at" in result

    def test_missing_brand(self):
        raw = {
            "id": "xyz789",
            "Name": "OG Kush",
            "brand": None,
            "category": "Flower",
        }
        result = normalize_product(raw, "Store A")
        assert result["brand"] == ""

    def test_empty_brand_dict(self):
        raw = {
            "id": "def456",
            "Name": "Gummies",
            "brand": {},
            "category": "Edible",
        }
        result = normalize_product(raw, "Store B")
        assert result["brand"] == ""

    def test_missing_fields_default(self):
        raw = {}
        result = normalize_product(raw, "Empty Store")

        assert result["product_id"] == ""
        assert result["name"] == ""
        assert result["brand"] == ""
        assert result["category"] == ""
        assert result["subcategory"] == ""
        assert result["strain_type"] == ""
        assert result["prices"] == []
        assert result["thc"] == ""
        assert result["cbd"] == ""
        assert result["is_sold_out"] is False

    def test_sold_out_flag(self):
        raw = {"id": "sold1", "Name": "Sold Item", "isSoldOut": True}
        result = normalize_product(raw, "Store C")
        assert result["is_sold_out"] is True

    def test_scraped_at_is_iso_format(self):
        raw = {"id": "time1", "Name": "Time Test"}
        result = normalize_product(raw, "Store D")
        assert "T" in result["scraped_at"]
        assert result["scraped_at"].endswith("+00:00")


class TestExtractDispensaryName:
    def test_simple_url(self):
        url = "https://dutchie.com/dispensary/green-leaf-store"
        assert extract_dispensary_name(url) == "Green Leaf Store"

    def test_nested_path(self):
        url = "https://dutchie.com/dispensary/best-buds/menu"
        assert extract_dispensary_name(url) == "Menu"

    def test_trailing_slash(self):
        url = "https://dutchie.com/dispensary/my-shop/"
        assert extract_dispensary_name(url) == "My Shop"

    def test_single_word(self):
        url = "https://dutchie.com/dispensary/greenstore"
        assert extract_dispensary_name(url) == "Greenstore"
