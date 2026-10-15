import json
import re
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from scrapling.spiders import Spider, Request, Response
from scrapling.fetchers import FetcherSession

import time
from core.logger import get_logger
from core.catalog_tracker import CatalogTracker
from core.raw_storage import create_raw_run, finalize_raw_run
from core.timer import timed_step

logger = get_logger("amazon_pdp_spider")

class AmazonPDP_Spider(Spider):
    name = "amazon_pdp_spider"
    # Fast fetcher for 2000 ASIN scale - old 3000 records were done with Fetcher
    concurrent_requests = 3
    download_delay = 0.5

    def configure_sessions(self, manager):
        manager.add(
            "fast_http",
            FetcherSession(
                impersonate="chrome",
                stealthy_headers=True,
                proxy=None,
                timeout=15,
                retries=2,
                retry_delay=1,
            ),
            lazy=True,
        )

    def __init__(self, products_data, mode="spec", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.products_data = products_data
        self.mode = mode  # 'spec' (full deep-dive) or 'monitor' (lightweight price/stock check)
        self.raw_run = create_raw_run(platform="amazon_in", source="pdp")
        self.tracker = CatalogTracker()

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
                    "search_category": prod.get('search_category', 'unknown'),
                    "leaf_node_id": prod.get('leaf_node_id', '')
                }
            )

    @timed_step("PDP Product Spec Parser")
    async def parse(self, response: Response):
        asin = response.meta.get("asin")
        category = response.meta.get("search_category")
        leaf_node_id = response.meta.get("leaf_node_id")

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
        # 1. Commercial Pricing (Live selling price, MRP, unit price)
        # ----------------------------------------------------------------------
        price = (
            response.css('#corePriceDisplay_desktop_feature_div .a-price:not(.a-text-price) span.a-offscreen::text').get()
            or response.css('#corePrice_desktop .a-price:not(.a-text-price) span.a-offscreen::text').get()
            or response.css('#apex_desktop .a-price:not(.a-text-price) span.a-offscreen::text').get()
            or response.css('.priceToPay span.a-offscreen::text').get()
            or response.css('.a-price.aok-align-center:not(.a-text-price) span.a-offscreen::text').get()
            or response.css('#priceblock_ourprice::text').get()
            or response.css('#priceblock_dealprice::text').get()
        )

        mrp = (
            response.css('span[data-a-strike="true"] span.a-offscreen::text').get()
            or response.css('.basisPrice span.a-offscreen::text').get()
            or response.css('#corePriceDisplay_desktop_feature_div .a-text-price[data-a-strike="true"] span.a-offscreen::text').get()
            or response.css('#corePriceDisplay_desktop_feature_div span.a-size-small.a-color-secondary span.a-price[data-a-strike="true"] span.a-offscreen::text').get()
            or response.css('.a-price.a-text-price[data-a-strike="true"] span.a-offscreen::text').get()
        )

        unit_price = (
            response.css('span.pricePerUnit span.a-price span.a-offscreen::text').get()
            or response.css('span.pricePerUnit::text').get()
            or response.css('#corePriceDisplay_desktop_feature_div span.a-size-small.a-color-secondary:contains("/")::text').get()
        )

        price = price.strip() if price else None
        mrp = mrp.strip() if mrp else None
        unit_price = " ".join(unit_price.split()) if unit_price else None

        # ----------------------------------------------------------------------
        # 2. Availability & Seller (Buy Box)
        # ----------------------------------------------------------------------
        availability = response.css('#availability span::text').get()
        availability = " ".join(availability.split()) if availability else None
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
            if not price and swatch_price:
                price = swatch_price.strip()
            if not mrp and swatch_mrp:
                mrp = swatch_mrp.strip()

        # Fallback to whole price
        if not price:
            whole = response.css('#corePriceDisplay_desktop_feature_div span.a-price-whole::text').get() or response.css('.priceToPay span.a-price-whole::text').get()
            if whole:
                price = f"₹{whole.strip().replace('.', '')}"

        # Discover sibling variation ASINs on this page
        available_variations = []
        sibling_asins_to_queue = []
        variation_elements = response.css('li[data-asin], div#tp-inline-twister-dim-values-container li, #variation_size_name li')
        for v in variation_elements:
            v_title = v.css('img::attr(alt)').get() or v.css('span.swatch-title-text-display::text').get() or v.css('span.a-size-base::text').get()
            v_asin = v.attrib.get('data-asin') or v.attrib.get('data-defaultasin')
            v_p = v.css('span.a-price:not(.a-text-price) span.a-offscreen::text').get() or v.css('span.a-price-whole::text').get()
            if v_title:
                v_title = " ".join(v_title.split())
                var_item = {"size": v_title}
                if v_asin:
                    v_asin = v_asin.strip().upper()
                    var_item["asin"] = v_asin
                    if v_asin != asin:
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
        bsr_links = response.css('#SalesRank a, #detailBullets_feature_div a[href*="bestsellers"], #detailBullets_feature_div a[href*="zgbs"]')
        for b in bsr_links:
            b_href = b.attrib.get('href', '')
            b_name = " ".join(b.css('::text').get("").split())
            m_bsr = re.search(r'/(?:zgbs|bestsellers)/[^/]+/(\d+)', b_href) or re.search(r'[?&]node=(\d+)', b_href)
            if m_bsr and b_name:
                bsr_categories.append({"name": b_name, "node_id": m_bsr.group(1)})

        # Product Details Table (Net Quantity, Item Weight, Manufacturer)
        details = {}
        rows = response.css('#productDetails_techSpec_section_1 tr') or response.css('#detailBullets_feature_div li')
        for row in rows:
            key = row.xpath('.//*[contains(@class, "a-text-bold")]//text() | .//th//text()').get()
            val = row.xpath('.//*[not(contains(@class, "a-text-bold"))]//text() | .//td//text()').getall()
            val = " ".join(val) if val else None
            if key and val:
                key = re.sub(r'[\u200f\u200e\s:]+', ' ', key).strip()
                val = re.sub(r'[\u200f\u200e\s]+', ' ', val).strip()
                if key and val:
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

        yield {
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OmniPrice Enterprise PDP Spider")
    parser.add_argument("--mode", choices=["spec", "monitor"], default="spec",
                        help="'spec' for new product specs; 'monitor' for existing product price/stock refresh")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of ASINs to scrape")
    parser.add_argument("--test", action="store_true", help="Run in test mode on sample diverse ASINs")
    args = parser.parse_args()

    tracker = CatalogTracker()
    logger.info(f"Starting PDP Spider in mode='{args.mode}' (Catalog size: {tracker.count():,} ASINs)...")

    if args.test:
        sample_asins = [
            {"asin": "B08JDK9JL7", "search_category": "fmcg_shampoo", "leaf_node_id": "1374411031"}, # Park Avenue Shampoo
            {"asin": "B015MXZOP0", "search_category": "fmcg_shampoo", "leaf_node_id": "1374411031"}, # Head & Shoulders
            {"asin": "B0BN6F4J51", "search_category": "fmcg_chips", "leaf_node_id": "4860045031"}    # Pringles
        ]
        products_to_scrape = sample_asins
        logger.info(f"Running TEST MODE on {len(products_to_scrape)} sample ASINs.")
    elif args.mode == "spec":
        products_to_scrape = tracker.get_unscraped_pdp_asins(limit=args.limit)
        logger.info(f"Loaded {len(products_to_scrape)} UNSCRAPED ASINs for full spec deep-dive.")
        if not products_to_scrape:
            # Fallback if catalog is completely up to date
            logger.info("All catalog ASINs have full PDP specs. Checking for stale items...")
            products_to_scrape = tracker.get_asins_due_for_monitoring(max_age_hours=24, limit=args.limit or 20)
    else:  # monitor mode
        products_to_scrape = tracker.get_asins_due_for_monitoring(max_age_hours=24, limit=args.limit)
        logger.info(f"Loaded {len(products_to_scrape)} existing ASINs due for price/stock monitoring.")

    if not products_to_scrape:
        logger.info("No ASINs to scrape at this time. Pipeline is up-to-date.")
    else:
        t_start = time.perf_counter()
        spider = AmazonPDP_Spider(products_data=products_to_scrape, mode=args.mode)
        result = spider.start()
        t_elapsed = time.perf_counter() - t_start

        if result.items:
            items_count = len(result.items)
            rate = items_count / t_elapsed if t_elapsed > 0 else 0
            logger.info(f"⏱️ [PERF SUMMARY] Crawled {items_count} PDP records in {t_elapsed:.2f}s ({rate:.1f} items/sec).")
            output_file = spider.raw_run.run_dir / f"pdp_{spider.raw_run.run_id}.jsonl"
            result.items.to_jsonl(str(output_file))
            manifest_file = finalize_raw_run(
                run=spider.raw_run,
                data_file=output_file,
                record_count=items_count,
                scraper_name=spider.name,
                extra_metadata={"crawl_mode": args.mode},
            )
            logger.info(f"Raw PDP data saved to {output_file}; manifest={manifest_file}")

            # Update lifecycle state in CatalogTracker
            scraped_asins = [it.get("asin") for it in result.items if it.get("asin")]
            if args.mode == "spec":
                tracker.mark_batch_pdp_scraped(scraped_asins)
            else:
                tracker.mark_batch_price_checked(scraped_asins)
            logger.info(f"Updated CatalogTracker lifecycle for {len(scraped_asins)} ASINs.")
        else:
            logger.warning(f"No items returned from PDP spider. (Elapsed: {t_elapsed:.2f}s).")
