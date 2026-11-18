//! parse_trade: no broker needed.
mod common;
use chrono::Duration;
use common::*;
use spike_detector::kafka::{Parsed, parse_trade};

fn json(price: &str, ts: &str) -> String {
    format!(r#"{{"i":1,"S":"AAPL","p":{price},"s":100,"t":"{ts}","x":"V","z":"C"}}"#)
}

#[test]
fn valid_trade_parses() {
    let now = at(0);
    let p = json("100.5", "2024-01-02T15:00:00Z");
    match parse_trade(Some(p.as_bytes()), now, 300) {
        Parsed::Trade(t) => {
            assert_eq!(t.symbol, "AAPL");
            assert_eq!(t.price, 100.5);
            assert_eq!(t.timestamp, at(0));
        }
        _ => panic!("expected Trade"),
    }
}

#[test]
fn malformed_payloads_do_not_panic() {
    let now = at(0);
    let cases: Vec<Option<&[u8]>> = vec![
        None,
        Some(b""),
        Some(b"not json"),
        Some(b"{}"),
        Some(b"null"),
        Some(b"[1,2,3]"),
        Some(br#"{"i":1,"S":"A","p":"abc","s":1,"t":"2024-01-02T15:00:00Z"}"#),
        Some(br#"{"i":1,"S":"A","p":1.0,"s":1,"t":"yesterday"}"#),
        Some(&[0xff, 0xfe, 0x00, 0x80]),
    ];
    for c in cases {
        assert!(matches!(parse_trade(c, now, 300), Parsed::Malformed), "case {c:?}");
    }
}

#[test]
fn future_skew_boundary() {
    let now = at(0);
    let ok = json("1.0", &(now + Duration::seconds(300)).to_rfc3339());
    assert!(matches!(parse_trade(Some(ok.as_bytes()), now, 300), Parsed::Trade(_)), "exactly at skew is allowed");
    let bad = json("1.0", &(now + Duration::seconds(301)).to_rfc3339());
    assert!(matches!(parse_trade(Some(bad.as_bytes()), now, 300), Parsed::Future));
}
