//! Section 9: "a saved fixture parses again with no network".
//!
//! This is the guarantee that makes replay real. If a stored payload cannot be
//! turned back into rows without the provider, the archive is a pile of JSON we
//! cannot use.

use fc_market::ids::Platform;
use fc_market::ingest::fixture::FixtureSource;
use fc_market::source::Source;
use std::path::PathBuf;

fn fixture_dir(name: &str) -> PathBuf {
    // Cargo runs a test with the package root as its working directory, so the
    // path to the repository's fixtures is relative to the manifest.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(name)
}

fn source() -> FixtureSource {
    FixtureSource::new(fixture_dir("fixture"))
}

#[tokio::test]
async fn a_saved_fixture_parses_again_with_no_network() {
    let source = source();
    let wanted = vec!["505120".to_string(), "201535".to_string()];

    let envelope = source.fetch_prices(&wanted).await.unwrap();
    let quotes = source.parse_prices(&envelope).unwrap();

    // Two cards, both platforms.
    assert_eq!(quotes.len(), 4);
    assert!(quotes.iter().all(|q| wanted.contains(&q.external_id)));
    assert!(quotes.iter().any(|q| q.platform == Platform::Playstation));
    assert!(quotes.iter().any(|q| q.platform == Platform::Pc));

    let zidane = quotes
        .iter()
        .find(|q| q.external_id == "505120" && q.platform == Platform::Playstation)
        .unwrap();
    assert_eq!(zidane.price, Some(1_420_000));
    assert_eq!(zidane.name.as_deref(), Some("Zinedine Zidane"));
    assert!(
        zidane.observed_at.is_some(),
        "the provider timestamp must survive the round trip"
    );
}

/// The envelope records what was asked for. Rule 1 sends one request per player
/// exactly when a response carries no identifier, and in that case the body
/// alone cannot be attributed to an asset on replay.
#[tokio::test]
async fn the_envelope_records_the_identifiers_that_were_requested() {
    let source = source();
    let wanted = vec!["505120".to_string()];
    let envelope = source.fetch_prices(&wanted).await.unwrap();

    assert_eq!(envelope.requested_ids, wanted);
    assert_eq!(envelope.http_status, 200);
    assert!(!envelope.sha256.is_empty());
}

/// An identifier the provider no longer knows simply does not come back. That
/// absence is coverage evidence, so the payload must not invent a record.
#[tokio::test]
async fn an_identifier_the_provider_does_not_know_returns_nothing() {
    let source = source();
    let envelope = source
        .fetch_prices(&["does-not-exist".to_string()])
        .await
        .unwrap();
    assert!(source.parse_prices(&envelope).unwrap().is_empty());
}

#[tokio::test]
async fn the_asset_list_and_the_card_attributes_parse_from_disk() {
    let source = source();

    let listing = source.fetch_asset_list().await.unwrap();
    let assets = source.parse_asset_list(&listing).unwrap();
    assert_eq!(assets.len(), 21);

    let ids: Vec<String> = assets.iter().map(|a| a.external_id.clone()).collect();
    let metadata = source.fetch_metadata(&ids).await.unwrap();
    let attributes = source.parse_metadata(&metadata).unwrap();
    assert_eq!(attributes.len(), 21);

    // The canonical domains we own, mapped at the edge.
    let zidane = attributes
        .iter()
        .find(|a| a.external_id == "505120")
        .unwrap();
    assert_eq!(zidane.version, "icon");
    assert_eq!(zidane.rating, Some(95));
    assert_eq!(zidane.playstyle_count, Some(4));

    assert!(
        attributes.iter().all(|a| a.ea_base_id.is_some()),
        "every card must carry the identifier of the real footballer"
    );
}

/// The second fixture is the next poll of the same cards. It exists so a test
/// can exercise a price that moved without editing the first one.
#[tokio::test]
async fn the_later_fixture_holds_the_same_cards_at_moved_prices() {
    let first = source();
    let later = FixtureSource::new(fixture_dir("fixture-later"));
    let wanted = vec!["201535".to_string()];

    let before = first
        .parse_prices(&first.fetch_prices(&wanted).await.unwrap())
        .unwrap();
    let after = later
        .parse_prices(&later.fetch_prices(&wanted).await.unwrap())
        .unwrap();

    assert_eq!(before.len(), after.len());
    assert_ne!(before[0].price, after[0].price, "the price must have moved");
    assert!(
        after[0].observed_at > before[0].observed_at,
        "and the provider timestamp must have advanced with it"
    );
}
