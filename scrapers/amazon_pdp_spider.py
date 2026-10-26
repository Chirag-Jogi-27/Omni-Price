import json
import re
import argparse
import time
import os
import gc
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from scrapling.spiders import Spider, Request, Response
from scrapling.fetchers import FetcherSession

from core.logger import get_logger
from core.catalog_tracker import CatalogTracker
from core.raw_storage import create_raw_run, finalize_raw_run
from core.timer import timed_step

logger = get_logger("amazon_pdp_spider")
logging.getLogger("scrapling").setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Canonical taxonomy (mirrors targets.json). Historical catalog-tracker seeds
# used ad-hoc codes (fmcg_chips, fmcg_shampoo, fmcg_toothpaste, ...) which is
# why amazon_product_snapshot contained BOTH naming schemes and downstream
# category analytics were at risk of splitting (e.g. 'fmcg_chips' vs
# 'Chips & Crisps'). Normalize at the source so search + PDP emit identical
# category labels.
# ---------------------------------------------------------------------------
CANONICAL_CATEGORIES = {
    "Chips & Crisps", "Biscuits & Cookies", "Tea & Coffee", "Oils & Ghee",
    "Shampoos", "Soaps & Body Wash", "Toothpastes", "Laundry Detergents",
    "Disposable Diapers",
}

LEGACY_CATEGORY_ALIASES = {
    "fmcg_chips": "Chips & Crisps",
    "fmcg_biscuits": "Biscuits & Cookies",
    "fmcg_cookies": "Biscuits & Cookies",
    "fmcg_tea": "Tea & Coffee",
    "fmcg_coffee": "Tea & Coffee",
    "fmcg_oil": "Oils & Ghee",
    "fmcg_oils": "Oils & Ghee",
    "fmcg_ghee": "Oils & Ghee",
    "fmcg_shampoo": "Shampoos",
    "fmcg_soap": "Soaps & Body Wash",
    "fmcg_toothpaste": "Toothpastes",
    "fmcg_detergent": "Laundry Detergents",
    "fmcg_detergents": "Laundry Detergents",
    "fmcg_diapers": "Disposable Diapers",
    "fmcg_diaper": "Disposable Diapers",
}


def normalize_category(raw):
    """Map legacy seed category codes to the canonical targets.json taxonomy.
    Unknown labels pass through unchanged (visible in data, never silently
    remapped)."""
    if not raw:
        return "unknown"
    c = str(raw).strip()
    if c in CANONICAL_CATEGORIES:
        return c
    low = c.lower()
    if low in LEGACY_CATEGORY_ALIASES:
        return LEGACY_CATEGORY_ALIASES[low]
    # case-insensitive canonical match (e.g. 'shampoos' -> 'Shampoos')
    for canon in CANONICAL_CATEGORIES:
        if canon.lower() == low:
            return canon
    return c


def get_amazon_clearance_cookies(proxy: str = None) -> str:
    """
    Performs a 1-second handshake with Amazon to obtain anti-bot clearance cookies
    (e.g., x-amz-captcha-1, x-amz-captcha-2) so PDP requests aren't intercepted
    by the 'Click to continue' interstitial challenge.
    """
    try:
        from curl_cffi import requests as cffi_requests
        s = cffi_requests.Session()
        proxies = {"http": proxy, "https": proxy} if proxy else None
        headers = {"referer": "https://www.google.com/"}
        
        # Probe endpoint to see if challenge is presented
        r = s.get(
            "https://www.amazon.in/errors_page/validateCaptcha",
            impersonate="chrome120",
            headers=headers,
            proxies=proxies,
            timeout=10,
        )
        inputs = dict(re.findall(r'<input\s+type="hidden"\s+name="([^"]+)"\s+value="([^"]*)"', r.text))
        if not inputs:
            r = s.get(
                "https://www.amazon.in/",
                impersonate="chrome120",
                headers=headers,
                proxies=proxies,
                timeout=10,
            )
            inputs = dict(re.findall(r'<input\s+type="hidden"\s+name="([^"]+)"\s+value="([^"]*)"', r.text))

        if inputs:
            s.get(
                "https://www.amazon.in/errors_page/validateCaptcha",
                params=inputs,
                impersonate="chrome120",
                proxies=proxies,
                timeout=10,
            )

        cookie_str = "; ".join([f"{k}={v}" for k, v in s.cookies.items()])
        if "amz" in cookie_str or "session" in cookie_str or "ubid" in cookie_str:
            logger.info("Successfully acquired Amazon clearance session cookies.")
            return cookie_str
    except Exception as e:
        logger.warning(f"Could not acquire Amazon clearance cookies automatically: {e}")
    return ""


class AmazonPDP_Spider(Spider):
    name = "amazon_pdp_spider"
    logging_level = logging.INFO
    concurrent_requests = 2
    download_delay = 1.0

    def configure_sessions(self, manager):
        proxy = os.getenv("SCRAPER_PROXY") or None
        headers = {}
        if getattr(self, "cookie_header", None):
            headers["Cookie"] = self.cookie_header

        manager.add(
            "fast_http",
            FetcherSession(
                impersonate="chrome",
                stealthy_headers=True,
                headers=headers,
                proxy=proxy,
                timeout=20,
                retries=2,
                retry_delay=1,
            ),
            default=True,
            lazy=True,
        )

    async def on_close(self):
        """Clean up session manager and release memory/connections immediately."""
        try:
            if hasattr(self, "_session_manager") and self._session_manager:
                await self._session_manager.close()
        except Exception:
            pass
        self.logger.debug("Spider and sessions closed cleanly.")

    def __init__(self, products_data, mode="spec", raw_run=None, output_file=None, cookie_header=None, *args, **kwargs):
        self.cookie_header = cookie_header
        self.products_data = products_data
        self.mode = mode  # 'spec' (full deep-dive) or 'monitor' (lightweight price/stock check)
        self.raw_run = raw_run or create_raw_run(platform="amazon_in", source="pdp")
        self.tracker = CatalogTracker()
        self.output_file = output_file or (self.raw_run.run_dir / f"pdp_{self.raw_run.run_id}.jsonl")
        self.items_written = 0
        self.missed_price_count = 0
        super().__init__(*args, **kwargs)

    async def start_requests(self):
        logger.info(f"Queuing {len(self.products_data)} PDP targets in mode='{self.mode}'...")
        for prod in self.products_data:
            asin = prod['asin'].strip().upper()
            url = f"https://www.amazon.in/dp/{asin}"
            yield Request(
                url,
                sid="fast_http",
                callback=self.parse,
                meta={
                    "asin": asin,
                    "search_category": normalize_category(prod.get('search_category')),
                    "leaf_node_id": prod.get('leaf_node_id', '')
                }
            )

    @timed_step("PDP Product Spec Parser")
    async def parse(self, response: Response):
        asin = response.meta.get("asin")
        category = response.meta.get("search_category")
        leaf_node_id = response.meta.get("leaf_node_id")

        # Check if Amazon returned a 404 Page Not Found (Deleted / Delisted ASIN)
        if response.status == 404:
            logger.warning(
                "Product removed from Amazon (404 Page Not Found) for ASIN=%s. Deactivating from queue.",
                asin,
            )
            self.tracker.deactivate_asin(asin)
            return

        # A challenge/blocked response can still produce an HTTP response, so
        # Scrapling would otherwise emit a misleading all-null PDP record.
        # Reject it before lifecycle state is marked as successfully scraped.
        title_probe = response.css('#productTitle::text').get()
        if not title_probe:
            debug_dir = Path("data_lake/debug/pdp_failures")
            debug_dir.mkdir(parents=True, exist_ok=True)
            debug_file = debug_dir / f"{asin}_{self.raw_run.run_id}.html"
            body = response.body
            if isinstance(body, str):
                debug_file.write_text(body, encoding="utf-8", errors="ignore")
            else:
                debug_file.write_bytes(body or b"")
            logger.error(
                "PDP quality failure for ASIN=%s: product title missing; "
                "likely Amazon challenge/blocked/incomplete response (url=%s, debug=%s)",
                asin,
                response.url,
                debug_file,
            )
            return

        # ----------------------------------------------------------------------
        # ----------------------------------------------------------------------
        # 1. Commercial Pricing (Live selling price, MRP, unit price)
        # ----------------------------------------------------------------------
        price = None
        for sel in [
            '#corePriceDisplay_desktop_feature_div .a-price:not(.a-text-price) span.a-offscreen::text',
            '#corePrice_desktop .a-price:not(.a-text-price) span.a-offscreen::text',
            '#apex_desktop .a-price:not(.a-text-price) span.a-offscreen::text',
            '.priceToPay span.a-offscreen::text',
            '.a-price.aok-align-center:not(.a-text-price) span.a-offscreen::text',
            '#priceblock_ourprice::text',
            '#priceblock_dealprice::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip() and val.strip().lower() != "null":
                price = val.strip()
                break

        mrp = None
        for sel in [
            'span[data-a-strike="true"] span.a-offscreen::text',
            '.basisPrice span.a-offscreen::text',
            '#corePriceDisplay_desktop_feature_div .a-text-price[data-a-strike="true"] span.a-offscreen::text',
            '#corePriceDisplay_desktop_feature_div span.a-size-small.a-color-secondary span.a-price[data-a-strike="true"] span.a-offscreen::text',
            '#apex_desktop .a-price.a-text-price[data-a-strike="true"] span.a-offscreen::text',
            '.a-price.a-text-price[data-a-strike="true"] span.a-offscreen::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip() and val.strip().lower() != "null":
                mrp = val.strip()
                break

        unit_price = None
        for sel in [
            'span.pricePerUnit span.a-price span.a-offscreen::text',
            'span.pricePerUnit::text',
            '#corePriceDisplay_desktop_feature_div span.a-size-small.a-color-secondary:contains("/")::text',
            '#apex_desktop .apex-priceperunit-value span.a-offscreen::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip() and val.strip().lower() != "null":
                unit_price = " ".join(val.split())
                break

        # ----------------------------------------------------------------------
        # 2. Availability & Seller (Buy Box)
        # ----------------------------------------------------------------------
        is_out_of_stock = bool(
            response.css('#outOfStock, .a-box-error').get()
            or response.css('#availability:contains("Currently unavailable"), #availability:contains("out of stock")').get()
        )
        has_see_all = bool(
            response.css('#buybox-see-all-buying-choices, #unqualifiedBuyBox_feature_div, a[href*="offer-listing"]').get()
        )

        availability = (
            response.css('#availability span.primary-availability-message::text').get()
            or response.css('#availability span::text').get()
            or response.css('#availability::text').get()
            or response.css('div[id*="availability"] span::text').get()
            or response.css('#outOfStock .a-box-inner span::text').get()
            or response.css('#outOfStock .a-alert-content::text').get()
        )
        if availability:
            availability = " ".join(availability.split())
            if not availability:
                availability = None

        seller = (
            response.css('#merchant-info a span::text').get()
            or response.css('#merchantInfoFeature_feature_div span.a-size-small a::text').get()
            or response.css('#tabular-buybox .tabular-buybox-text[tabular-attribute-name="Sold by"] a::text').get()
        )
        seller = " ".join(seller.split()) if seller else None

        # ----------------------------------------------------------------------
        # 3. Twister Variations & Sibling ASIN Auto-Discovery Loop
        # ----------------------------------------------------------------------
        active_size = (
            response.css('#inline-twister-dim-title-size_name span.a-color-base::text').get()
            or response.css('#variation_size_name span.selection::text').get()
            or response.css('div#inline-twister-row-size_name span.a-form-label + span::text').get()
            or response.css('li.swatchSelect span.swatch-title-text-display::text').get()
        )
        active_size = " ".join(active_size.split()) if active_size else None

        # Active swatch price fallbacks
        selected_swatch = response.css('li.swatchSelect, li[class*="swatchSelect"], li.a-button-selected')
        if selected_swatch:
            swatch_price = selected_swatch.css('.slot-price span.a-price:not(.a-text-price) span.a-offscreen::text').get()
            swatch_mrp = selected_swatch.css('span[data-a-strike="true"] span.a-offscreen::text').get()
            if not price and swatch_price and swatch_price.strip():
                price = swatch_price.strip()
            if not mrp and swatch_mrp and swatch_mrp.strip():
                mrp = swatch_mrp.strip()

        # Fallback to whole price digits if offscreen was blank or whitespace
        if not price:
            whole = (
                response.css('#corePriceDisplay_desktop_feature_div span.a-price-whole::text').get()
                or response.css('.priceToPay span.a-price-whole::text').get()
                or response.css('#apex_desktop span.a-price-whole::text').get()
            )
            if whole and whole.strip():
                price = f"₹{whole.strip().replace('.', '')}"

        if price:
            availability_status = "in_stock"
        elif is_out_of_stock or (availability and any(t in availability.lower() for t in ("unavailable", "out of stock"))):
            availability_status = "currently_unavailable"
            if not availability:
                availability = "Currently unavailable."
        elif has_see_all or (availability and "see all buying options" in availability.lower()):
            availability_status = "no_featured_offer"
            if not availability:
                availability = "See All Buying Options"
        elif availability:
            avail_low = availability.lower()
            if any(t in avail_low for t in ("in stock", "only", "left in", "order soon")):
                availability_status = "in_stock"
            else:
                availability_status = "other"
        else:
            availability_status = "unknown"

        price_el_present = bool(
            response.css(
                '#corePriceDisplay_desktop_feature_div, #corePrice_desktop, '
                '#apex_desktop, .priceToPay, #priceblock_ourprice, #priceblock_dealprice'
            ).get()
        )

        if price:
            price_missing_reason = None
            price_block_snippet = None
        elif availability_status == "currently_unavailable":
            price_missing_reason = "currently_unavailable"
            price_block_snippet = None
        elif availability_status == "no_featured_offer":
            price_missing_reason = "no_featured_offer"
            price_block_snippet = None
        elif not price_el_present:
            price_missing_reason = "no_price_section"
            price_block_snippet = (response.css('#buybox, #corePriceDisplay_desktop_feature_div').get() or "")[:400] or None
        else:
            price_missing_reason = "selector_miss"
            price_block_snippet = (response.css('#corePriceDisplay_desktop_feature_div, #apex_desktop, .priceToPay').get() or "")[:400] or None

        # Single automatic retry for unexplained price misses (fresh fetch)
        if (
            not price
            and price_missing_reason in ("no_price_section", "selector_miss")
            and response.meta.get("retry_attempt", 0) < 1
        ):
            logger.warning(
                "PDP price missing for ASIN=%s (reason=%s, status=%s) — retrying once (attempt 2)",
                asin, price_missing_reason, availability_status,
            )
            yield Request(
                response.url,
                sid="fast_http",
                callback=self.parse,
                dont_filter=True,
                meta={
                    "asin": asin,
                    "search_category": category,
                    "leaf_node_id": leaf_node_id,
                    "retry_attempt": 1,
                },
            )
            return

        # Discover sibling variation ASINs on this page
        available_variations = []
        sibling_asins_to_queue = []
        variation_elements = response.css('li[data-asin], div#tp-inline-twister-dim-values-container li, #variation_size_name li')
        for v in variation_elements:
            classes = v.attrib.get("class", "")
            is_disabled = (
                "swatch-unavailable" in classes
                or "a-button-unavailable" in classes
                or v.attrib.get("data-a-strike") == "true"
                or v.attrib.get("aria-disabled") == "true"
            )
            v_title = v.css('img::attr(alt)').get() or v.css('span.swatch-title-text-display::text').get() or v.css('span.a-size-base::text').get()
            v_asin = v.attrib.get('data-asin') or v.attrib.get('data-defaultasin')
            v_p = v.css('span.a-price:not(.a-text-price) span.a-offscreen::text').get() or v.css('span.a-price-whole::text').get()
            if v_title:
                v_title = " ".join(v_title.split())
                var_item = {"size": v_title}
                if v_asin:
                    v_asin = v_asin.strip().upper()
                    var_item["asin"] = v_asin
                    # Only queue available sibling ASINs to avoid queuing dead/discontinued stock
                    if v_asin != asin and not is_disabled:
                        sibling_asins_to_queue.append({
                            "asin": v_asin,
                            "search_category": category,
                            "leaf_node_id": leaf_node_id
                        })
                if v_p:
                    var_item["price"] = v_p.strip()
                if var_item not in available_variations:
                    available_variations.append(var_item)

        # Autonomous recursive feedback loop: Register discovered siblings into SQLite state coordinator!
        if sibling_asins_to_queue:
            self.tracker.register_discovered_asins(sibling_asins_to_queue, source="sibling_variation")

        # ----------------------------------------------------------------------
        # 4. Master Product Specs & Breadcrumb Taxonomy Hierarchy (Spec Mode)
        # ----------------------------------------------------------------------
        title = response.css('#productTitle::text').get()
        title = " ".join(title.split()) if title else None

        brand_link = response.css('#bylineInfo::text').get() or response.css('#bylineInfo_feature_div a::text').get()
        brand = " ".join(brand_link.split()).replace("Visit the ", "").replace(" Store", "").replace("Brand: ", "") if brand_link else None

        main_image = response.css('#landingImage::attr(src)').get()
        bullets = [" ".join(b.split()) for b in response.css('#feature-bullets li span.a-list-item::text').getall() if b.strip()]

        # Breadcrumb Category Tree with exact Browse Node IDs
        category_hierarchy = []
        bc_elements = response.css('#wayfinding-breadcrumbs_feature_div ul li span a, #wayfinding-breadcrumbs_feature_div a.a-link-normal')
        for link in bc_elements:
            b_name = " ".join(link.css('::text').get("").split())
            b_href = link.attrib.get('href', '')
            m_node = re.search(r'[?&]node=(\d+)', b_href)
            node_id = m_node.group(1) if m_node else None
            if b_name and b_name not in [c["name"] for c in category_hierarchy]:
                category_hierarchy.append({
                    "name": b_name,
                    "node_id": node_id,
                    "url": b_href
                })
        breadcrumbs = [c["name"] for c in category_hierarchy]

        # Ratings & Reviews
        rating = response.css('#acrPopover::attr(title)').get() or response.css('span[data-hook="rating-out-of-text"]::text').get()
        review_count = response.css('#acrCustomerReviewText::text').get()

        # BSR Category Node IDs
        bsr_categories = []
        bsr_links = response.css('#SalesRank a, #detailBullets_feature_div a[href*="bestsellers"], #detailBullets_feature_div a[href*="zgbs"], #productDetails_db_sections a[href*="bestsellers"], #productDetails_db_sections a[href*="zgbs"]')
        for b in bsr_links:
            b_href = b.attrib.get('href', '')
            b_name = " ".join(b.css('::text').get("").split())
            m_bsr = re.search(r'/(?:zgbs|bestsellers)/[^/]+/(\d+)', b_href) or re.search(r'[?&]node=(\d+)', b_href)
            if m_bsr and b_name:
                bsr_categories.append({"name": b_name, "node_id": m_bsr.group(1)})

        # ------------------------------------------------------------------
        # 5. Product Details Table — Multi-Layout Extraction
        # ------------------------------------------------------------------
        details = {}
        rows = (
            response.css('#productDetails_techSpec_section_1 tr')
            or response.css('#productDetails_techSpec_section_2 tr')
            or response.css('#detailBullets_feature_div li')
            or response.css('#productDetails_db_sections .a-section .a-row')
            or response.css('#productDetails_detailBullets_sections1 tr')
        )
        for row in rows:
            key = row.xpath('.//*[contains(@class, "a-text-bold")]//text() | .//th//text()').get()
            val = row.xpath('.//*[not(contains(@class, "a-text-bold"))]//text() | .//td//text()').getall()
            val = " ".join(val) if val else None
            if key and val:
                key = re.sub(r'[\u200f\u200e\s:]+', ' ', key).strip()
                val = re.sub(r'[\u200f\u200e\s]+', ' ', val).strip()
                if key and val:
                    key_lower = key.lower()
                    if "country" in key_lower and "origin" in key_lower:
                        key = "Country of Origin"
                    elif "manufacturer" in key_lower:
                        key = "Manufacturer"
                    elif "net" in key_lower and "quantity" in key_lower:
                        key = "Net Quantity"
                    elif "item" in key_lower and "weight" in key_lower:
                        key = "Item Weight"
                    elif "item" in key_lower and "dimension" in key_lower:
                        key = "Item Dimensions LxWxH"
                    elif "package" in key_lower and "quantity" in key_lower:
                        key = "Item Package Quantity"
                    elif key_lower.startswith("number") and "item" in key_lower:
                        key = "Number of Items"
                    elif "included" in key_lower and "component" in key_lower:
                        key = "Included Components"
                    elif "packer" in key_lower:
                        key = "Packer"
                    elif "importer" in key_lower:
                        key = "Importer"
                    elif "generic" in key_lower and "name" in key_lower:
                        key = "Generic Name"
                    details[key] = val

        # Multi-pack detection from Included Components or table
        pack_qty = details.get("Item Package Quantity") or details.get("Number of Items")
        if not pack_qty and details.get("Included Components"):
            comp = details.get("Included Components")
            m_comp = re.search(r'(\d+)\s*(?:unit|bottle|pack|piece|can|count)', comp, re.IGNORECASE)
            if m_comp:
                pack_qty = m_comp.group(1)

        # Extract leaf node ID from breadcrumbs if available
        final_leaf_node_id = (category_hierarchy[-1].get("node_id") if category_hierarchy else None) or leaf_node_id or ""

        # Derive pack_count from active variation when details table is sparse (e.g. 107 g Pack of 3)
        derived_pack = None
        if active_size:
            m_pack = re.search(r"Pack of\s*(\d+)", active_size, re.IGNORECASE)
            if m_pack:
                try:
                    derived_pack = int(m_pack.group(1))
                except ValueError:
                    derived_pack = None
        effective_pack = pack_qty or (str(derived_pack) if derived_pack and derived_pack > 1 else None)

        if not brand:
            mfr = details.get("Manufacturer", "")
            if mfr:
                brand = mfr.split(",")[0].strip()
        if not brand and title:
            first_word = title.split()[0].rstrip(",!-–") if title.split() else None
            if first_word and len(first_word) > 1:
                brand = first_word

        record = {
            "asin": asin,
            "search_category": category,
            "leaf_node_id": final_leaf_node_id,
            "pdp_title": title,
            "pdp_brand": brand,
            "active_variation_size": active_size,
            "raw_price": price,
            "raw_mrp": mrp,
            "raw_unit_price": unit_price,
            "pack_count": effective_pack,
            "available_variations": available_variations,
            "availability": availability,
            "availability_status": availability_status,
            "price_missing_reason": price_missing_reason,
            "price_block_snippet": price_block_snippet,
            "seller": seller,
            "main_image": main_image,
            "bullet_points": bullets,
            "category_hierarchy": category_hierarchy,
            "bsr_categories": bsr_categories,
            "raw_rating": rating,
            "raw_review_count": review_count,
            "product_details_table": details,
            "crawl_mode": self.mode,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": response.url
        }

        # STREAM-WRITE IMMEDIATELY TO DISK (Crash resilience: never hold in RAM only)
        with open(self.output_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.items_written += 1

        if not price:
            self.missed_price_count += 1

        # Mark lifecycle immediately in SQLite
        if self.mode == "spec":
            self.tracker.mark_batch_pdp_scraped([asin])
        else:
            self.tracker.mark_batch_price_checked([asin])

        # Yield lightweight stub instead of giant record to keep Python engine RAM near 0 MB
        yield {"asin": asin, "status": "saved"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OmniPrice Enterprise PDP Spider")
    parser.add_argument("--mode", choices=["spec", "monitor"], default="spec",
                        help="'spec' for new product specs; 'monitor' for existing product price/stock refresh")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of ASINs to scrape")
    parser.add_argument("--batch-size", type=int, default=20, help="Number of ASINs per batch to recycle memory (default: 20)")
    parser.add_argument("--max-age-days", type=int, default=7, help="Max age in days before re-scraping PDP specs (default: 7)")
    parser.add_argument("--test", action="store_true", help="Run in test mode on sample diverse ASINs")
    args = parser.parse_args()

    tracker = CatalogTracker()
    logger.info(f"Starting PDP Spider in mode='{args.mode}' (Catalog size: {tracker.count():,} ASINs)...")

    if args.test:
        sample_asins = [
            {"asin": "B08JDK9JL7", "search_category": "Shampoos", "leaf_node_id": "1374411031"},
            {"asin": "B015MXZOP0", "search_category": "Shampoos", "leaf_node_id": "1374411031"},
            {"asin": "B0BN6F4J51", "search_category": "Chips & Crisps", "leaf_node_id": "4860045031"}
        ]
        products_to_scrape = sample_asins
        logger.info(f"Running TEST MODE on {len(products_to_scrape)} sample ASINs.")
    elif args.mode == "spec":
        products_to_scrape = tracker.get_unscraped_pdp_asins(limit=args.limit)
        logger.info(f"Loaded {len(products_to_scrape)} ASINs for full spec deep-dive (new + anomalies + tiered rolling).")
        if not products_to_scrape:
            logger.info("All catalog ASINs have fresh PDP specs. Checking for stale price items...")
            products_to_scrape = tracker.get_asins_due_for_monitoring(max_age_hours=24, limit=args.limit or 20)
    else:  # monitor mode
        products_to_scrape = tracker.get_asins_due_for_monitoring(max_age_hours=24, limit=args.limit)
        logger.info(f"Loaded {len(products_to_scrape)} existing ASINs due for price/stock monitoring.")

    if not products_to_scrape:
        logger.info("No ASINs to scrape at this time. Pipeline is up-to-date.")
    else:
        batch_size = args.batch_size
        total_asins = len(products_to_scrape)
        total_batches = (total_asins + batch_size - 1) // batch_size
        logger.info(
            f"🚀 Loaded {total_asins} ASINs. Executing in auto-recycled batches of {batch_size} (Zero-RAM mode with fast_http)..."
        )

        # Single master raw run and output file for the entire execution
        raw_run = create_raw_run(platform="amazon_in", source="pdp")
        output_file = raw_run.run_dir / f"pdp_{raw_run.run_id}.jsonl"
        total_written = 0
        t_start = time.perf_counter()

        proxy = os.getenv("SCRAPER_PROXY") or None
        active_cookie_header = get_amazon_clearance_cookies(proxy)

        try:
            for batch_idx in range(0, total_asins, batch_size):
                chunk = products_to_scrape[batch_idx : batch_idx + batch_size]
                batch_num = (batch_idx // batch_size) + 1

                logger.info(
                    f"\n🔄 [BATCH {batch_num}/{total_batches}] Starting HTTP session for {len(chunk)} ASINs..."
                )

                spider = AmazonPDP_Spider(
                    products_data=chunk,
                    mode=args.mode,
                    raw_run=raw_run,
                    output_file=output_file,
                    cookie_header=active_cookie_header
                )

                try:
                    spider.start()
                except Exception as e:
                    logger.error(f"Batch {batch_num} error: {e}", exc_info=True)
                finally:
                    total_written += spider.items_written
                    logger.info(
                        f"✅ [BATCH {batch_num}/{total_batches}] Done ({spider.items_written} items saved). "
                        f"Cumulative saved: {total_written}/{total_asins}"
                    )
                    # Refresh cookies if a batch returned 0 items
                    if spider.items_written == 0 and len(chunk) > 5:
                        logger.info("Batch yielded 0 items, refreshing clearance cookies for next batch...")
                        active_cookie_header = get_amazon_clearance_cookies(proxy)
                    del spider

                # Force Python and OS garbage collection between batches
                gc.collect()
                time.sleep(1.0)

        finally:
            t_elapsed = time.perf_counter() - t_start
            if total_written > 0:
                rate = total_written / t_elapsed if t_elapsed > 0 else 0
                logger.info(
                    f"\n🎉 [CRAWL COMPLETED] Safely saved {total_written} PDP records in {t_elapsed:.2f}s ({rate:.1f} items/sec)."
                )
                manifest_file = finalize_raw_run(
                    run=raw_run,
                    data_file=output_file,
                    record_count=total_written,
                    scraper_name="amazon_pdp_spider",
                    extra_metadata={"crawl_mode": args.mode, "batches": total_batches, "batch_size": batch_size},
                )
                logger.info(f"Raw data safely stored: {output_file}; manifest={manifest_file}")
            else:
                logger.warning(f"No items returned from PDP spider. (Elapsed: {t_elapsed:.2f}s).")