from __future__ import annotations

from paired_opener.kanglong.reporter import summarize_batch_ledger_costs


def test_batch_reporter_keeps_adverse_wear_and_improvement_separate() -> None:
    entries = [
        {
            "account_id": "a1",
            "entry_type": "fee",
            "fee_amount": "2",
            "payload": {"position_side": "LONG", "round_index": 0},
        },
        {
            "account_id": "a1",
            "entry_type": "price_wear",
            "price_wear": "5",
            "payload": {
                "position_side": "LONG",
                "round_index": 0,
                "wear_category": "spread_cost",
                "adverse": "5",
                "improvement": "3",
            },
        },
        {
            "account_id": "a1",
            "entry_type": "price_wear",
            "price_wear": "4",
            "payload": {
                "position_side": "SHORT",
                "round_index": 1,
                "wear_category": "alignment_cost",
                "adverse": "4",
                "improvement": "0",
            },
        },
    ]
    report = summarize_batch_ledger_costs(entries)
    assert report["total_fee_cost"] == "2"
    assert report["total_adverse_wear"] == "9"
    assert report["total_price_improvement"] == "3"
    assert report["spread_cost"] == "5"
    assert report["alignment_cost"] == "4"
