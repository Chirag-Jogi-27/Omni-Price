import sqlite3
import glob
import json
from pathlib import Path
from datetime import datetime, timezone
from core.logger import get_logger

import hashlib
import re

logger = get_logger("catalog_tracker")

DEFAULT_DB_PATH = Path("data_lake/metadata/catalog_state.db")

class CatalogTracker:
    """
    Enterprise SQLite-backed Catalog State & Lifecycle Coordinator.
    
    Coordinates two distinct workloads:
    1. Discovery Engine: Captures new ASINs & detects search shelf anomalies via shelf_hash.
    2. Tiered Monitoring Engine: Schedules PDP deep-dives using a 4-tier priority queue:
       - Tier 1: Brand new ASINs (never scraped)
       - Tier 2: Shelf anomalies (title / price / raw_unit_price shifted today)
       - Tier 3: High-velocity items (bought badge / 5K+ reviews / sponsored) every 3 days
       - Tier 4: Long-tail items every 7 days
    """
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path, timeout=30.0)

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Master ASIN lifecycle table with shelf_hash, anomaly, and velocity tiering
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS catalog_asin (
                    asin TEXT PRIMARY KEY,
                    search_category TEXT,
                    leaf_node_id TEXT,
                    discovery_source TEXT DEFAULT 'search_shelf',
                    first_seen_at TIMESTAMP,
                    last_pdp_scraped_at TIMESTAMP,
                    last_price_checked_at TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    shelf_hash TEXT,
                    is_shelf_anomaly INTEGER DEFAULT 0,
                    velocity_tier INTEGER DEFAULT 2
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_pdp ON catalog_asin(last_pdp_scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_price ON catalog_asin(last_price_checked_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_category ON catalog_asin(search_category)")

            # In-place migrations for existing database columns (must run before creating indexes on them)
            for col, col_type in [
                ("shelf_hash", "TEXT"),
                ("is_shelf_anomaly", "INTEGER DEFAULT 0"),
                ("velocity_tier", "INTEGER DEFAULT 2")
            ]:
                try:
                    cursor.execute(f"ALTER TABLE catalog_asin ADD COLUMN {col} {col_type}")
                except sqlite3.OperationalError:
                    pass  # Column already exists

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_anomaly ON catalog_asin(is_shelf_anomaly)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_tier ON catalog_asin(velocity_tier)")

            # Legacy migration: If old 'scraped_asins' table exists, migrate data into 'catalog_asin'
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scraped_asins'")
            if cursor.fetchone():
                cursor.execute("""
                    INSERT OR IGNORE INTO catalog_asin (asin, search_category, first_seen_at, last_pdp_scraped_at, last_price_checked_at)
                    SELECT asin, category, scraped_at, scraped_at, scraped_at FROM scraped_asins
                """)
            conn.commit()

    @staticmethod
    def compute_shelf_hash(title: str, price: any, raw_unit_price: any) -> str:
        """
        Computes deterministic SHA256 fingerprint for search shelf change detection.
        Uses '__NULL__' sentinel pattern so null -> null is stable, while
        title edits, price shifts, and unit_price changes trigger an anomaly.
        Excludes volatile MRP and ratings.
        """
        clean_title = (title or "").strip().lower()
        p_str = "__NULL__" if price is None else str(price).strip()
        up_str = "__NULL__" if raw_unit_price is None else str(raw_unit_price).strip()
        fingerprint = f"{clean_title}||{p_str}||{up_str}"
        return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_review_count(reviews: any) -> int:
        """Parse Amazon review count strings like '(14.2K)', '(1.5K)', '(943)', '943', or ints."""
        if isinstance(reviews, (int, float)):
            return int(reviews)
        if not isinstance(reviews, str):
            return 0
        s = reviews.strip().strip("()")
        s = re.sub(r"[^\d.KkMm]", "", s)  # keep digits, dots, K/M suffixes
        if not s:
            return 0
        try:
            if "K" in s or "k" in s:
                return int(float(s.upper().replace("K", "")) * 1000)
            if "M" in s or "m" in s:
                return int(float(s.upper().replace("M", "")) * 1_000_000)
            return int(float(s))
        except ValueError:
            return 0

    @staticmethod
    def compute_velocity_tier(sales_volume_tag: str = None, reviews: any = 0, is_sponsored: bool = False) -> int:
        """
        Calibrated Velocity Tiering:
        Tier 1 (High Velocity - ~23% of catalog, re-crawled every 3 days):
          - sales_volume_tag >= "10K+ bought" (10K+, 20K+, 40K+, 1L+, etc.)
          - OR review_count > 5000 (established FMCG staples)
          - OR (is_sponsored AND review_count > 1000) [avoids tier flapping from ephemeral ad spend]
        Tier 2 (Long Tail - ~77% of catalog, re-crawled every 7 days):
          - Everything else (sustainable crawl budget, zero IP ban risk)
        """
        tag = (sales_volume_tag or "").strip()
        rev_count = CatalogTracker._parse_review_count(reviews)

        # High velocity tags (case-folded)
        high_velocity_tags = ["10k+", "20k+", "30k+", "40k+", "50k+", "1l+", "2l+", "5l+"]
        is_high_volume = any(t in tag.lower() for t in high_velocity_tags)
        
        # High review count (> 5,000)
        is_high_reviews = rev_count > 5000
        
        # Sponsored booster ONLY with social proof (> 1000 reviews) to eliminate ephemeral tier flapping
        is_sponsored_booster = bool(is_sponsored and rev_count > 1000)

        if is_high_volume or is_high_reviews or is_sponsored_booster:
            return 1
        return 2

    def register_discovered_asins(self, items: list[dict], source: str = "search_shelf"):
        """
        Registers discovered ASINs with shelf_hash change detection and velocity tiering.
        If shelf_hash changes from yesterday, sets is_shelf_anomaly = 1 to trigger immediate PDP scrape.
        """
        if not items:
            return
        now = datetime.now(timezone.utc).isoformat()
        records = []
        for item in items:
            asin = item.get("asin")
            if not asin:
                continue
            title = item.get("raw_title") or item.get("title") or ""
            price = item.get("raw_price") if item.get("raw_price") is not None else item.get("price")
            unit_price = item.get("raw_unit_price") if item.get("raw_unit_price") is not None else item.get("unit_price")
            shelf_h = self.compute_shelf_hash(title, price, unit_price)
            tier = self.compute_velocity_tier(
                sales_volume_tag=item.get("sales_volume_tag"),
                reviews=item.get("raw_review_count") or item.get("reviews"),
                is_sponsored=bool(item.get("is_sponsored"))
            )
            records.append((
                asin.strip().upper(),
                item.get("search_category", "unknown"),
                item.get("leaf_node_id", ""),
                source,
                now,
                now,
                shelf_h,
                tier
            ))

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO catalog_asin (
                    asin, search_category, leaf_node_id, discovery_source, 
                    first_seen_at, last_price_checked_at, shelf_hash, is_shelf_anomaly, velocity_tier
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(asin) DO UPDATE SET
                    last_price_checked_at = excluded.last_price_checked_at,
                    velocity_tier = excluded.velocity_tier,
                    is_shelf_anomaly = CASE 
                        WHEN catalog_asin.shelf_hash IS NOT NULL AND catalog_asin.shelf_hash != excluded.shelf_hash THEN 1
                        ELSE catalog_asin.is_shelf_anomaly
                    END,
                    shelf_hash = excluded.shelf_hash
            """, records)
            conn.commit()
        logger.info(f"Registered/Refreshed {len(records)} ASINs with shelf_hash & velocity tiering.")

    def get_unscraped_pdp_asins(self, limit: int = None) -> list[dict]:
        """
        Returns ASINs needing full PDP specification scraping:
        1. Brand new ASINs (last_pdp_scraped_at IS NULL)
        2. Shelf anomalies detected today (is_shelf_anomaly = 1) -> Immediate Day 1 crawl!
        3. Tier 1 High-velocity stale items (> 3 days old)
        4. Tier 2 Long-tail stale items (> 7 days old)
        """
        query = """
            SELECT asin, search_category, leaf_node_id 
            FROM catalog_asin 
            WHERE is_active = 1 
              AND (
                  last_pdp_scraped_at IS NULL 
                  OR is_shelf_anomaly = 1
                  OR (velocity_tier = 1 AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 3 * 86400)
                  OR (velocity_tier = 2 AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 7 * 86400)
              )
            ORDER BY 
              CASE 
                  WHEN last_pdp_scraped_at IS NULL THEN 1
                  WHEN is_shelf_anomaly = 1 THEN 2
                  WHEN velocity_tier = 1 THEN 3
                  ELSE 4
              END ASC,
              last_pdp_scraped_at ASC
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            return [{"asin": r[0], "search_category": r[1], "leaf_node_id": r[2]} for r in rows]

    def get_asins_due_for_monitoring(self, max_age_hours: int = 24, limit: int = None) -> list[dict]:
        """
        Returns ASINs whose price/stock check is older than max_age_hours.
        Used by the Daily Monitoring Spider.
        """
        query = """
            SELECT asin, search_category, leaf_node_id 
            FROM catalog_asin 
            WHERE is_active = 1 
              AND (last_price_checked_at IS NULL 
                   OR (strftime('%s', 'now') - strftime('%s', last_price_checked_at)) > ? * 3600)
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (max_age_hours,))
            rows = cursor.fetchall()
            return [{"asin": r[0], "search_category": r[1], "leaf_node_id": r[2]} for r in rows]

    def mark_batch_pdp_scraped(self, asins: list[str]):
        """Marks ASINs as having their master PDP specs scraped and resets anomaly flag."""
        if not asins:
            return
        now = datetime.now(timezone.utc).isoformat()
        records = [(now, now, a.strip().upper()) for a in asins if a]
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                UPDATE catalog_asin 
                SET last_pdp_scraped_at = ?, last_price_checked_at = ?, is_shelf_anomaly = 0
                WHERE asin = ?
            """, records)
            conn.commit()
        logger.info(f"Marked {len(records)} ASINs as Full PDP Scraped (anomaly flags reset).")

    def mark_batch_price_checked(self, asins: list[str]):
        """Updates last_price_checked_at timestamp for daily monitoring runs."""
        if not asins:
            return
        now = datetime.now(timezone.utc).isoformat()
        records = [(now, a.strip().upper()) for a in asins if a]
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                UPDATE catalog_asin 
                SET last_price_checked_at = ?
                WHERE asin = ?
            """, records)
            conn.commit()

    # --------------------------------------------------------------------------
    # Backward Compatibility Methods for Legacy Callers
    # --------------------------------------------------------------------------
    def is_scraped(self, asin: str) -> bool:
        """Checks if an ASIN has already received a full PDP spec scrape."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM catalog_asin WHERE asin = ? AND last_pdp_scraped_at IS NOT NULL LIMIT 1", (asin.strip().upper(),))
            return cursor.fetchone() is not None

    def get_all_scraped_asins(self) -> set[str]:
        """Returns all ASINs that have completed a full PDP spec scrape."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT asin FROM catalog_asin WHERE last_pdp_scraped_at IS NOT NULL")
            return {row[0] for row in cursor.fetchall()}

    def filter_unscraped(self, products: list[dict]) -> list[dict]:
        """Filters input list to items that have not yet had their PDP scraped."""
        scraped = self.get_all_scraped_asins()
        return [p for p in products if p.get("asin", "").strip().upper() not in scraped]

    def count(self) -> int:
        """Returns total unique ASINs tracked in catalog."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM catalog_asin")
            return cursor.fetchone()[0]

    def get_total_tracked_count(self) -> int:
        """Alias for count()."""
        return self.count()

    def count_pdp_scraped(self) -> int:
        """Returns total ASINs with completed PDP specs."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM catalog_asin WHERE last_pdp_scraped_at IS NOT NULL")
            return cursor.fetchone()[0]
