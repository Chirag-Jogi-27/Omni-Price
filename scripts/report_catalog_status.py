#!/usr/bin/env python3
"""
OmniPrice Catalog & Crawl Lifecycle Diagnostic Report.

Run anytime to inspect:
- Total unique ASINs in catalog
- PDP Scraped vs Remaining (with visual progress bar)
- Velocity Tiers (Tier 1: 3-day high velocity vs Tier 2: 7-day long tail)
- Pending Priority Queue breakdown for next PDP crawl
- Top FMCG categories breakdown
"""

import sys
import sqlite3
from pathlib import Path

# Paths
DB_PATH = Path(__file__).resolve().parent.parent / "data_lake" / "metadata" / "catalog_state.db"

# ANSI Colors for Terminal Output
BOLD = "\033[1m"
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
DIM = "\033[2m"
RESET = "\033[0m"


def format_progress_bar(pct: float, width: int = 30) -> str:
    filled = int(round(width * (pct / 100.0)))
    empty = width - filled
    bar = f"{GREEN}{'█' * filled}{DIM}{'░' * empty}{RESET}"
    return f"[{bar}] {BOLD}{pct:.1f}%{RESET}"


def print_report():
    if not DB_PATH.exists():
        print(f"{RED}❌ Error: Database not found at {DB_PATH}{RESET}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. High-level counts
    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1")
    total_asins = cursor.fetchone()[0]

    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1 AND last_pdp_scraped_at IS NOT NULL")
    scraped_asins = cursor.fetchone()[0]

    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1 AND last_pdp_scraped_at IS NULL")
    never_scraped = cursor.fetchone()[0]

    # Scraped in last 24h
    cursor.execute("""
        SELECT count(*) FROM catalog_asin 
        WHERE last_pdp_scraped_at IS NOT NULL 
          AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) <= 86400
    """)
    scraped_last_24h = cursor.fetchone()[0]

    pct = (scraped_asins / total_asins * 100.0) if total_asins else 0.0

    # 2. Velocity Tier breakdown
    cursor.execute("""
        SELECT velocity_tier, count(*) 
        FROM catalog_asin 
        WHERE is_active = 1 
        GROUP BY velocity_tier 
        ORDER BY velocity_tier
    """)
    tier_counts = {row[0]: row[1] for row in cursor.fetchall()}
    tier1_count = tier_counts.get(1, 0)
    tier2_count = tier_counts.get(2, 0)

    # 3. Pending Queue Breakdown (Aligned with CatalogTracker 5-tier priority)
    # Never scraped breakdown: search_shelf vs sibling_variation
    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1 AND last_pdp_scraped_at IS NULL AND discovery_source = 'search_shelf'")
    never_scraped_search = cursor.fetchone()[0]

    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1 AND last_pdp_scraped_at IS NULL AND discovery_source = 'sibling_variation'")
    never_scraped_siblings = cursor.fetchone()[0]

    # Priority 1: Shelf Anomalies (price/pack shifted today)
    cursor.execute("SELECT count(*) FROM catalog_asin WHERE is_active = 1 AND is_shelf_anomaly = 1")
    anomalies_count = cursor.fetchone()[0]

    # Priority 2: Brand New Search Shelf Items
    new_search_count = never_scraped_search

    # Priority 3: Tier 1 Stale (> 3 days)
    cursor.execute("""
        SELECT count(*) FROM catalog_asin 
        WHERE is_active = 1 
          AND velocity_tier = 1 
          AND last_pdp_scraped_at IS NOT NULL 
          AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 3 * 86400
    """)
    tier1_stale = cursor.fetchone()[0]

    # Priority 4: Brand New Sibling Variations
    new_siblings_count = never_scraped_siblings

    # Priority 5: Tier 2 Stale (> 7 days)
    cursor.execute("""
        SELECT count(*) FROM catalog_asin 
        WHERE is_active = 1 
          AND velocity_tier = 2 
          AND last_pdp_scraped_at IS NOT NULL 
          AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 7 * 86400
    """)
    tier2_stale = cursor.fetchone()[0]

    # Total currently due for PDP crawl
    cursor.execute("""
        SELECT count(*) FROM catalog_asin 
        WHERE is_active = 1 
          AND (
              last_pdp_scraped_at IS NULL 
              OR is_shelf_anomaly = 1 
              OR (velocity_tier = 1 AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 3 * 86400)
              OR (velocity_tier = 2 AND (strftime('%s', 'now') - strftime('%s', last_pdp_scraped_at)) > 7 * 86400)
          )
    """)
    total_queued = cursor.fetchone()[0]

    # 4. Top 5 Categories
    cursor.execute("""
        SELECT 
            search_category,
            count(*) as total,
            sum(CASE WHEN last_pdp_scraped_at IS NOT NULL THEN 1 ELSE 0 END) as scraped
        FROM catalog_asin
        WHERE is_active = 1
        GROUP BY search_category
        ORDER BY total DESC
        LIMIT 6
    """)
    categories = cursor.fetchall()

    conn.close()

    # --------------------------------------------------------------------------
    # Output Rendering
    # --------------------------------------------------------------------------
    print(f"\n{BOLD}{CYAN}========================================================================{RESET}")
    print(f"{BOLD}{CYAN}📊  OmniPrice Catalog & Crawl Lifecycle Diagnostic Report{RESET}")
    print(f"{BOLD}{CYAN}========================================================================{RESET}")

    print(f"\n{BOLD}🎯 Overall Catalog Coverage:{RESET}")
    print(f"   • Total Discovered ASINs:    {BOLD}{total_asins:,}{RESET}")
    print(f"   • Completed PDP Scrapes:     {GREEN}{BOLD}{scraped_asins:,}{RESET}")
    print(f"   • Remaining (Never Scraped): {YELLOW}{BOLD}{never_scraped:,}{RESET} {DIM}({never_scraped_search:,} search shelf | {never_scraped_siblings:,} sibling swatches){RESET}")
    print(f"   • Scraped in Last 24 Hours:  {CYAN}{BOLD}{scraped_last_24h:,}{RESET}")
    print(f"   • Coverage Progress:         {format_progress_bar(pct)}")

    print(f"\n{BOLD}⏱️  Velocity Tiers (Crawl Frequency Schedule):{RESET}")
    t1_pct = (tier1_count / total_asins * 100) if total_asins else 0
    t2_pct = (tier2_count / total_asins * 100) if total_asins else 0
    print(f"   • {BOLD}Tier 1 (High Velocity - 3-Day Cycle):{RESET} {CYAN}{tier1_count:,}{RESET} ASINs ({t1_pct:.1f}%)")
    print(f"     {DIM}Criteria: 10K+ bought/mo, >5,000 reviews, or sponsored staples{RESET}")
    print(f"   • {BOLD}Tier 2 (Long Tail - 7-Day Cycle):{RESET}     {tier2_count:,} ASINs ({t2_pct:.1f}%)")
    print(f"     {DIM}Criteria: Sustainable crawl budget for standard FMCG catalog{RESET}")

    print(f"\n{BOLD}📋  PDP Crawl Queue (Priority Sorting for Next Run):{RESET}")
    print(f"   • {BOLD}Total Items Currently Due:{RESET}  {MAGENTA}{BOLD}{total_queued:,}{RESET} ASINs")
    print(f"     ├── {BOLD}Priority 1 (Shelf Anomalies):{RESET}      {RED if anomalies_count else GREEN}{anomalies_count:,}{RESET} ASINs (price/pack shifted today)")
    print(f"     ├── {BOLD}Priority 2 (New Search Shelf):{RESET}     {YELLOW}{new_search_count:,}{RESET} ASINs (live search listings)")
    print(f"     ├── {BOLD}Priority 3 (Stale Tier 1):{RESET}         {CYAN}{tier1_stale:,}{RESET} ASINs (> 3 days old staples)")
    print(f"     ├── {BOLD}Priority 4 (New Sibling Variants):{RESET} {DIM}{new_siblings_count:,}{RESET} ASINs (discovered variant swatches)")
    print(f"     └── {BOLD}Priority 5 (Stale Tier 2):{RESET}         {tier2_stale:,} ASINs (> 7 days old long-tail)")

    print(f"\n{BOLD}📁  Top Category Coverage Breakdown:{RESET}")
    print(f"   {'Category':<32} {'Discovered':<12} {'PDP Scraped':<12} {'Coverage':<10}")
    print(f"   {'-'*32} {'-'*12} {'-'*12} {'-'*10}")
    for cat, total, sc in categories:
        cat_pct = (sc / total * 100) if total else 0
        cat_name = (cat[:29] + "...") if len(cat) > 32 else cat
        print(f"   {cat_name:<32} {total:<12} {sc:<12} {GREEN if cat_pct >= 70 else YELLOW}{cat_pct:>5.1f}%{RESET}")

    print(f"\n{DIM}SQLite DB: {DB_PATH}{RESET}")
    print(f"{BOLD}{CYAN}========================================================================{RESET}\n")


if __name__ == "__main__":
    print_report()
