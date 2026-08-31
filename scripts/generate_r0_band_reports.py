#!/usr/bin/env python3
"""
scripts/generate_r0_band_reports.py

Generate band-specific R0 analysis reports (M1-M12) from the full consolidated reports.
Extracts each band's section from the detailed/comprehensive reports and creates
individual HTML files that can be served via /api/v1/backtest/html-reports/{report_name}_{band}.

Usage:
    python3 scripts/generate_r0_band_reports.py [--detailed] [--comprehensive] [--all]

    If no args, generates both report types for all bands (--all).
"""

import re
from pathlib import Path
from typing import List, Optional

# Map band IDs to their display names
BAND_NAMES = {
    'M1': 'Ranks 1-50',
    'M2': 'Ranks 1-75',
    'M3': 'Ranks 51-100',
    'M4': 'Ranks 76-160',
    'M5': 'Ranks 101-150',
    'M6': 'Ranks 151-200',
    'M7': 'Ranks 161-275',
    'M8': 'Ranks 201-300',
    'M9': 'Ranks 276-550',
    'M10': 'Ranks 301-500',
    'M11': 'Ranks 501-800',
    'M12': 'Ranks 551-800',
}

REPORTS_DIR = Path(__file__).parent.parent / 'backtest' / 'reports'


def extract_band_from_html(html_content: str, band_id: str, report_type: str = 'detailed') -> Optional[str]:
    """
    Extract a specific band's section from a consolidated HTML report.

    Handles two HTML structures:
    - detailed: <div class="band-container">...<div class="band-title">BAND M{id}</div>...</div>
    - comprehensive: <div class="band-section">...<div class="band-title">M{id}</div>...</div>

    Returns the HTML with a single band, or None if band not found.
    """
    band_section = None

    if report_type == 'detailed':
        # Detailed report: look for band-container with BAND M{id}
        band_pattern = rf'<div class="band-container">.*?<div class="band-title">BAND {band_id}</div>.*?</div>\s*</div>'
        match = re.search(band_pattern, html_content, re.DOTALL)
        if match:
            band_section = match.group(0)
    else:
        # Comprehensive report: find band-section with M{id}, extract until next band-section or end
        start_pattern = rf'<div class="band-section">\s*<div class="band-title">{band_id}</div>'
        start_match = re.search(start_pattern, html_content, re.DOTALL)
        if start_match:
            start_pos = start_match.start()
            # Find the next band-section or end of content
            next_section_match = re.search(r'<div class="band-section">', html_content[start_match.end():])
            if next_section_match:
                end_pos = start_match.end() + next_section_match.start()
            else:
                end_pos = len(html_content)

            band_section = html_content[start_pos:end_pos].rstrip()

    if not band_section:
        return None

    # Extract the header and basic structure
    header_match = re.search(r'<head>.*?</head>', html_content, re.DOTALL)
    if not header_match:
        return None

    header = header_match.group(0)

    # Build new HTML with just this band
    # Show actual backtest execution date and backtest period
    backtest_execution_date = "2026-08-25"
    backtest_period = "2009-01-01 to 2026-08-26"

    new_html = f"""<!DOCTYPE html>
<html>
{header}
<body>
<div class="container">
    <div class="header">
        <h1>R0 Strategy Analysis - Band {band_id} Report</h1>
        <p>{BAND_NAMES.get(band_id, 'Market Cap Band')}</p>
        <div class="timestamp">Executed: {backtest_execution_date} | Period: {backtest_period}</div>
    </div>
    <div class="content">
        <div class="section">
            {band_section}
        </div>
    </div>
</div>
</body>
</html>"""

    return new_html


def generate_band_reports(
    report_type: str = 'detailed',
    bands: Optional[List[str]] = None,
) -> None:
    """
    Generate band-specific reports from a consolidated report.

    Args:
        report_type: 'detailed' or 'comprehensive'
        bands: List of band IDs (M1-M12) to generate. If None, generates all.
    """
    if bands is None:
        bands = [f'M{i}' for i in range(1, 13)]

    # Read the source consolidated report
    if report_type == 'detailed':
        source_file = REPORTS_DIR / 'r0_band_analysis_detailed.html'
    else:
        source_file = REPORTS_DIR / 'r0_comprehensive_band_analysis_full.html'

    if not source_file.exists():
        print(f"❌ Source report not found: {source_file}")
        return

    print(f"📖 Reading source report: {source_file.name}")
    html_content = source_file.read_text(encoding='utf-8')

    success_count = 0
    for band_id in bands:
        band_html = extract_band_from_html(html_content, band_id, report_type)

        if band_html is None:
            print(f"⚠️  Band {band_id} not found in {report_type} report")
            continue

        # Write band-specific report (API expects: r0_band_analysis_detailed_{BAND}, etc.)
        if report_type == 'detailed':
            output_file = REPORTS_DIR / f'r0_band_analysis_detailed_{band_id}.html'
        else:
            output_file = REPORTS_DIR / f'r0_comprehensive_band_analysis_full_{band_id}.html'
        output_file.write_text(band_html, encoding='utf-8')
        print(f"✅ Generated: {output_file.name}")
        success_count += 1

    if success_count > 0:
        print(f"\n✨ {success_count}/{len(bands)} band reports generated successfully")
    else:
        print("\n❌ No bands were found in the source report")


def main() -> None:
    """Parse CLI arguments and generate reports."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Generate band-specific R0 analysis reports (M1-M12)'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Generate band-specific detailed reports',
    )
    parser.add_argument(
        '--comprehensive',
        action='store_true',
        help='Generate band-specific comprehensive reports',
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Generate both detailed and comprehensive reports (default if no args)',
    )
    parser.add_argument(
        '--bands',
        default='M1,M2,M3,M4,M5,M6,M7,M8,M9,M10,M11,M12',
        help='Comma-separated list of bands to generate (default: all)',
    )

    args = parser.parse_args()
    bands = [b.strip() for b in args.bands.split(',')]

    # Default to --all if no flags specified
    if not (args.detailed or args.comprehensive or args.all):
        args.all = True

    print(f"🎯 Generating reports for bands: {', '.join(bands)}\n")

    if args.detailed or args.all:
        print("📊 Generating detailed band reports...")
        generate_band_reports('detailed', bands)

    if args.comprehensive or args.all:
        print("\n📊 Generating comprehensive band reports...")
        generate_band_reports('comprehensive', bands)

    print("\n✨ Done!")


if __name__ == '__main__':
    main()
