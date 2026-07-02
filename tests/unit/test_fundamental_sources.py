from pathlib import Path

from ingestion.fundamentals.sources import CsvFundamentalSourceAdapter, merge_fundamental_rows


def test_csv_adapter_prefers_authoritative_source_and_falls_back(tmp_path: Path) -> None:
    csv_path = tmp_path / "fundamentals.csv"
    csv_path.write_text(
        "ticker,metric,as_of_date,value,source,confidence\n"
        "RELIANCE,revenue,2024-03-31,100,official,0.95\n"
        "RELIANCE,revenue,2024-03-31,96,alternate,0.75\n"
        "RELIANCE,ebitda,2024-03-31,20,alternate,0.75\n",
        encoding="utf-8",
    )

    adapter = CsvFundamentalSourceAdapter(csv_path, preferred_source_order=["official", "alternate"])
    rows = adapter.fetch_ticker_history("RELIANCE", as_of="2024-03-31", lookback_years=1)

    merged = merge_fundamental_rows(rows, preferred_source_order=["official", "alternate"])

    assert len(merged) == 2
    revenue = next(item for item in merged if item["metric"] == "revenue")
    ebitda = next(item for item in merged if item["metric"] == "ebitda")
    assert revenue["value"] == 100.0
    assert revenue["source"] == "official"
    assert ebitda["value"] == 20.0
    assert ebitda["source"] == "alternate"
