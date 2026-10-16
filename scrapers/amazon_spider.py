import re
import argparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from scrapling.spiders import Spider, Request, Response
from scrapling.fetchers import AsyncStealthySession
import time

from core.logger import get_logger
from core.config import get_targets
from core.exceptions import DataQualityError
from core.catalog_tracker import CatalogTracker
from core.raw_storage import create_raw_run, finalize_raw_run
from core.timer import timed_step

logger = get_logger("amazon_bronze_spider")

BASE_URL = "https://www.amazon.in"
DEFAULT_MAX_PAGES = 10
DEBUG_DIR = Path("data_lake/debug")

# review count regex (e.g. "1,234", "2.3K", "(5K+)")
REVIEW_RE = re.compile(r"^\(?[\d.,]+\s*[KkMm]?\+?\)?$")
# per-unit price text (e.g. "₹42.80/100 ml", "(₹82.55 /100 g)")
UNIT_PRICE_RE = re.compile(r"₹\s*[\d,]+(?:\.\d+)?\s*/\s*\d*\s*[A-Za-z]+")

def _clean(v):
    if v is None:
        return None
    v = " ".join(str(v).split())
    return v or None

def _first(item, *selectors):
    """Return first non-empty cleaned value from a list of CSS selectors."""
    for sel in selectors:
        v = _clean(item.css(sel).get())
        if v:
            return v
    return None


class AmazonOPI_Spider(Spider):
    name = "amazon_bronze_spider"
    # Hardened settings to bypass Akamai Bot Manager & IP rate-limits
    concurrent_requests = 1
    download_delay = 2.0

    def configure_sessions(self, manager):
        profile_dir = Path("data_lake/metadata/browser_profile")
        profile_dir.mkdir(parents=True, exist_ok=True)
        manager.add(
            "stealth_browser",
            AsyncStealthySession(
                headless=True,
                disable_resources=True,
                block_webrtc=True,
                hide_canvas=True,
                load_dom=True,
                user_data_dir=str(profile_dir.resolve()),
                timeout=30000,
            ),
            lazy=True
        )

    def __init__(self, max_pages=DEFAULT_MAX_PAGES, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = max_pages
        self.raw_run = create_raw_run(platform="amazon_in", source="search")
        self.ingestion_id = self.raw_run.run_id

    async def start_requests(self):
        targets = get_targets()
        for item in targets.get("amazon_in", []):
            url = item["url"]
            micro_category = item.get("micro_category") or item.get("category", "unknown")
            vertical_category = item.get("vertical_category", "FMCG")
            vertical_node_id = item.get("vertical_node_id", "")
            subcategory = item.get("subcategory", "")
            leaf_node_id = item.get("leaf_node_id", "")

            logger.info(f"Queuing Leaf Node Shelf: [{vertical_category} > {subcategory} > {micro_category}] (node={leaf_node_id}) -> {url}")
            yield Request(
                url,
                sid="stealth_browser",
                callback=self.parse,
                meta={
                    "vertical_category": vertical_category,
                    "vertical_node_id": vertical_node_id,
                    "subcategory": subcategory,
                    "micro_category": micro_category,
                    "leaf_node_id": leaf_node_id,
                    "category": micro_category,
                    "page": 1,
                },
            )

    # ------------------------------------------------------------------ field extractors
    @staticmethod
    def _title(item):
        t = _first(item, "h2[aria-label]::attr(aria-label)")
        if t:
            return re.sub(r'^(Sponsored Ad\s*[-–:]\s*)+', '', t, flags=re.IGNORECASE).strip()
        parts = item.xpath(".//h2//text()").getall()
        parts = [p.strip() for p in parts if p.strip() and p.strip() != "Sponsored"]
        clean_text = _clean(" ".join(parts))
        if clean_text:
            return re.sub(r'^(Sponsored Ad\s*[-–:]\s*)+', '', clean_text, flags=re.IGNORECASE).strip()
        return None

    @staticmethod
    def _price(item):
        return _first(item, "span.a-price:not(.a-text-price) span.a-offscreen::text")

    @staticmethod
    def _mrp(item):
        recipe = item.css('[data-cy="price-recipe"], .puis-price-instructions-style')
        recipe_text = " ".join([t.strip() for t in recipe.xpath('.//text()').getall() if t.strip()])
        m = re.search(r'M\.R\.P[:\s]*₹?\s*([\d,]+(?:\.\d{2})?)', recipe_text, re.IGNORECASE)
        if m:
            return f"₹{m.group(1)}"
        mrp = item.css('span[data-a-strike="true"] span.a-offscreen::text, span.a-price[data-a-strike="true"] span.a-offscreen::text').get()
        return _clean(mrp)

    @staticmethod
    def _unit_price(item):
        recipe = item.css('[data-cy="price-recipe"], .puis-price-instructions-style')
        recipe_text = " ".join([t.strip() for t in recipe.xpath('.//text()').getall() if t.strip()])
        m = re.search(r'\(?\s*(₹\s*[\d,]+(?:\.\d+)?\s*/\s*[\d\w\s]+)\)?', recipe_text)
        if m:
            return m.group(1).strip()
        blob = " ".join(item.xpath('.//span[contains(@class,"a-color-secondary")]//text()').getall())
        m2 = UNIT_PRICE_RE.search(blob)
        return m2.group(0).strip() if m2 else None

    @staticmethod
    def _discount(item):
        recipe = item.css('[data-cy="price-recipe"], .puis-price-instructions-style')
        recipe_text = " ".join([t.strip() for t in recipe.xpath('.//text()').getall() if t.strip()])
        m_disc = re.search(r'\(?(\d+%\s*off)\)?', recipe_text, re.IGNORECASE)
        return m_disc.group(1).strip() if m_disc else None

    @staticmethod
    def _badge(item):
        badge = item.css('span.a-badge-text::text, [data-a-badge-type="status"] .a-badge-text::text').get()
        if badge and "in cart" not in badge.lower():
            return _clean(badge)
        text_blob = " ".join(item.xpath('.//text()').getall())
        for b in ["Amazon's Choice", "Best Seller", "Limited time deal", "Deal of the Day", "Great Indian Festival", "Mega Festival Deal"]:
            if b.lower() in text_blob.lower():
                return b
        return None

    @staticmethod
    def _rating(item):
        return _first(item, "i.a-icon-star-small span.a-icon-alt::text", "span.a-icon-alt::text")

    @staticmethod
    def _review_count(item):
        v = _first(
            item,
            '[data-csa-c-content-id="alf-customer-ratings-count-component"] span.s-underline-text::text',
            'a[href*="customerReviews"] span.s-underline-text::text',
            'a[aria-label*="rating"] span.s-underline-text::text',
        )
        return v if v and REVIEW_RE.match(v) else None

    @staticmethod
    def _is_sponsored(item):
        if item.css(".puis-sponsored-label-text, .s-sponsored-label-info-icon"):
            return True
        return bool(item.xpath('.//span[normalize-space(text())="Sponsored"]'))

    @staticmethod
    def _sales_volume(item):
        for s in item.xpath('.//span[contains(@class,"a-color-secondary")]//text()').getall():
            if "bought in past" in s.lower():
                return _clean(s)
        return None



    # ------------------------------------------------------------------ parse
    @timed_step("Search Shelf Page Parser")
    async def parse(self, response: Response):
        category = response.meta.get("category", "unknown")
        micro_category = response.meta.get("micro_category", category)
        vertical_category = response.meta.get("vertical_category", "FMCG")
        subcategory = response.meta.get("subcategory", "")
        current_page = response.meta.get("page", 1)
        scraped_at = datetime.now(timezone.utc).isoformat()

        products = response.css('div[data-component-type="s-search-result"]')
        logger.info(f"Page {current_page} [{micro_category}]: Found {len(products)} result cards.")

        if len(products) == 0:
            debug_dir = DEBUG_DIR / "search_failures"
            debug_dir.mkdir(parents=True, exist_ok=True)
            debug_file = debug_dir / f"{micro_category.replace(' ', '_')}_{self.raw_run.run_id}_p{current_page}.html"
            body = response.body
            if isinstance(body, str):
                debug_file.write_text(body, encoding="utf-8", errors="ignore")
            else:
                debug_file.write_bytes(body or b"")
            logger.error(f"Challenge/blocked search page for {micro_category} Page {current_page} (debug={debug_file})")
            raise DataQualityError(f"SILENT FAILURE DETECTED on {micro_category} Page {current_page}! No products found. debug={debug_file}")

        missing_titles = 0
        skipped_no_asin = 0
        emitted = 0
        fill = {k: 0 for k in ("price", "mrp", "unit_price", "discount", "rating", "reviews", "sales_volume", "badge")}

        for pos, item in enumerate(products, start=1):
            asin = (item.attrib.get("data-asin") or "").strip().upper()
            if not asin:
                skipped_no_asin += 1
                continue

            title = self._title(item)
            if not title:
                missing_titles += 1
                continue

            price = self._price(item)
            mrp = self._mrp(item)
            unit_price = self._unit_price(item)
            discount = self._discount(item)
            badge = self._badge(item)
            rating = self._rating(item)
            reviews = self._review_count(item)
            sales_volume = self._sales_volume(item)
            sponsored = self._is_sponsored(item)

            for k, v in (("price", price), ("mrp", mrp), ("unit_price", unit_price),
                         ("discount", discount), ("rating", rating), ("reviews", reviews),
                         ("sales_volume", sales_volume), ("badge", badge)):
                fill[k] += bool(v)

            emitted += 1
            yield {
                "ingestion_id": self.ingestion_id,
                "platform": "amazon_in",
                "vertical_category": vertical_category,
                "subcategory": subcategory,
                "micro_category": micro_category,
                "search_category": micro_category,  # conformed silver join key
                "asin": asin,
                "raw_title": title,
                "raw_price": price,
                "raw_mrp": mrp,
                "raw_unit_price": unit_price,
                "raw_discount": discount,
                "raw_badge": badge,
                "raw_rating": rating,
                "raw_review_count": reviews,
                "raw_image_url": _first(item, "img.s-image::attr(src)"),
                "sales_volume_tag": sales_volume,
                "raw_coupon_badge": _first(item, "span.s-coupon-highlight-color::text"),
                "raw_sponsored_tag": "Sponsored" if sponsored else "Organic",
                "is_sponsored": sponsored,
                "page": current_page,
                "position_on_page": pos,
                "scraped_at": scraped_at,
                "source_url": response.url,
            }

        if emitted:
            rates = ", ".join(f"{k}={v / emitted:.0%}" for k, v in fill.items())
            logger.info(f"Page {current_page} [{micro_category}] fill rates: {rates} "
                        f"(emitted={emitted}, no_asin={skipped_no_asin})")

        if missing_titles / len(products) > 0.5:
            raise DataQualityError(f"ABORT: >50% missing titles on {micro_category} Page {current_page}.")

        if current_page < self.max_pages:
            next_page_link = response.css("a.s-pagination-next::attr(href)").get()
            if next_page_link:
                logger.info(f"Pagination: Moving to page {current_page + 1} for {micro_category}")
                yield Request(
                    urljoin(BASE_URL, next_page_link),
                    sid="stealth_browser",
                    callback=self.parse,
                    meta={
                        "vertical_category": vertical_category,
                        "subcategory": subcategory,
                        "micro_category": micro_category,
                        "category": micro_category,
                        "page": current_page + 1,
                    },
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OmniPrice Bronze Shelf Ingestion Spider")
    parser.add_argument("--pages", type=int, default=DEFAULT_MAX_PAGES, help="Max pages per category")
    args = parser.parse_args()

    t_start = time.perf_counter()
    logger.info(f"Starting OmniPrice Bronze Ingestion (max_pages={args.pages})...")
    spider = AmazonOPI_Spider(max_pages=args.pages)
    result = spider.start()
    t_elapsed = time.perf_counter() - t_start

    if result.items:
        items_count = len(result.items)
        rate = items_count / t_elapsed if t_elapsed > 0 else 0
        logger.info(f"⏱️ [PERF SUMMARY] Crawled {items_count} products in {t_elapsed:.2f}s ({rate:.1f} items/sec).")
        output_file = spider.raw_run.run_dir / f"search_{spider.raw_run.run_id}.jsonl"
        result.items.to_jsonl(str(output_file))
        manifest_file = finalize_raw_run(
            run=spider.raw_run,
            data_file=output_file,
            record_count=items_count,
            scraper_name=spider.name,
            extra_metadata={"max_pages": args.pages},
        )
        logger.info(f"Raw data saved to {output_file}; manifest={manifest_file}")

        # Automatically coordinate discovered ASINs into CatalogTracker with shelf_hash & velocity tiering
        tracker = CatalogTracker()
        tracker.register_discovered_asins([it for it in result.items if it.get("asin")], source="search_shelf")
        logger.info(f"Indexed all discovered shelf ASINs into CatalogTracker state with shelf_hash.")
    else:
        logger.warning(f"No items scraped. (Elapsed: {t_elapsed:.2f}s). Check logs for errors.")
