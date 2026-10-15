import sqlite3
import glob
import json
from pathlib import Path
from datetime import datetime, timezone
from core.logger import get_logger

logger = get_logger("catalog_tracker")

DEFAULT_DB_PATH = Path("data_lake/metadata/catalog_state.db")

class CatalogTracker:
    """
    Enterprise SQLite-backed Catalog State & Lifecycle Coordinator.
    
    Coordinates two distinct workloads:
    1. Discovery Engine: Captures new ASINs (from Search Shelves & Twister Variations) needing full spec crawls.
    2. Monitoring Engine: Schedules existing ASINs for daily price, stock, and BSR refreshes.
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
            
            # Master ASIN lifecycle table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS catalog_asin (
                    asin TEXT PRIMARY KEY,
                    search_category TEXT,
                    leaf_node_id TEXT,
                    discovery_source TEXT DEFAULT 'search_shelf',
                    first_seen_at TIMESTAMP,
                    last_pdp_scraped_at TIMESTAMP,
                    last_price_checked_at TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_pdp ON catalog_asin(last_pdp_scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_price ON catalog_asin(last_price_checked_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_category ON catalog_asin(search_category)")

            # Legacy migration: If old 'scraped_asins' table exists, migrate data into 'catalog_asin'
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scraped_asins'")
            if cursor.fetchone():
                cursor.execute("""
                    INSERT OR IGNORE INTO catalog_asin (asin, search_category, first_seen_at, last_pdp_scraped_at, last_price_checked_at)
                    SELECT asin, category, scraped_at, scraped_at, scraped_at FROM scraped_asins
                """)
            conn.commit()

    def register_discovered_asins(self, items: list[dict], source: str = "search_shelf"):
        """
        Registers newly discovered ASINs from Search Shelves or Sibling Variations.
        If the ASIN is already known, updates its last_price_checked_at timestamp.
        """
        if not items:
            return
        now = datetime.now(timezone.utc).isoformat()
        records = [
            (
                item["asin"].strip().upper(),
                item.get("search_category", "unknown"),
                item.get("leaf_node_id", ""),
                source,
                now,
                now
            )
            for item in items if item.get("asin")
        ]
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO catalog_asin (asin, search_category, leaf_node_id, discovery_source, first_seen_at, last_price_checked_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(asin) DO UPDATE SET
                    last_price_checked_at = excluded.last_price_checked_at
            """, records)
            conn.commit()
        logger.info(f"Registered/Refreshed {len(records)} ASINs from source='{source}'.")

    def get_unscraped_pdp_asins(self, limit: int = None) -> list[dict]:
        """
        Returns ASINs that have NEVER had their full PDP specs scraped.
        Used by the Full PDP Spec Spider.
        """
        query = """
            SELECT asin, search_category, leaf_node_id 
            FROM catalog_asin 
            WHERE last_pdp_scraped_at IS NULL AND is_active = 1
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
        """Marks ASINs as having their master PDP specs successfully scraped."""
        if not asins:
            return
        now = datetime.now(timezone.utc).isoformat()
        records = [(now, now, a.strip().upper()) for a in asins if a]
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                UPDATE catalog_asin 
                SET last_pdp_scraped_at = ?, last_price_checked_at = ?
                WHERE asin = ?
            """, records)
            conn.commit()
        logger.info(f"Marked {len(records)} ASINs as Full PDP Scraped.")

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
