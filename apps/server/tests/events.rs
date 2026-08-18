//! The event calendar. Every test names a way a study could be misled.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::analytics::events::{self, NewEvent};

fn drop_at(hours_ago: i64) -> NewEvent {
    NewEvent {
        kind: "content_drop".into(),
        name: "Team of the Week 42".into(),
        occurred_at: Utc::now() - Duration::hours(hours_ago),
        ended_at: None,
        note: None,
        supersedes: None,
    }
}

#[tokio::test]
async fn an_event_is_recorded_and_read_back() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let saved = events::record(&db.pool, game.id, &drop_at(6))
        .await
        .unwrap();
    assert_eq!(saved.kind, "content_drop");

    let known = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert_eq!(known.len(), 1);
    assert_eq!(known[0].name, "Team of the Week 42");
    db.cleanup().await;
}

/// A study groups on kind. "TOTW" beside "totw" would split one series into two
/// and nothing would raise it, which is why `version` is folded the same way.
#[tokio::test]
async fn the_kind_is_folded_so_one_series_cannot_split_in_two() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    for raw in ["Content Drop", "content drop", "CONTENT_DROP"] {
        let mut e = drop_at(6);
        e.kind = raw.into();
        e.name = format!("Release {raw}");
        events::record(&db.pool, game.id, &e).await.unwrap();
    }
    let known = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert!(
        known.iter().all(|e| e.kind == "content_drop"),
        "every spelling must fold to one kind: {:?}",
        known.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    db.cleanup().await;
}

/// The question a study asks is what we KNEW then, not what had happened by
/// then. An event recorded today about last Tuesday was not available to a study
/// run on Monday, and counting it is hindsight.
#[tokio::test]
async fn an_event_recorded_late_is_invisible_to_an_earlier_study() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    // Happened three days ago, recorded a moment ago.
    events::record(&db.pool, game.id, &drop_at(72))
        .await
        .unwrap();

    let earlier = events::known_at(&db.pool, game.id, Utc::now() - Duration::hours(24))
        .await
        .unwrap();
    assert!(
        earlier.is_empty(),
        "a study run yesterday could not have known: {earlier:?}"
    );

    let now = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert_eq!(now.len(), 1, "and it is known now");
    db.cleanup().await;
}

/// A time learned more precisely is a new row, not an edit. The old row stays
/// readable so a study run before the correction still reproduces.
#[tokio::test]
async fn a_correction_supersedes_rather_than_edits() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let first = events::record(&db.pool, game.id, &drop_at(6))
        .await
        .unwrap();

    let before_correction = Utc::now();
    let mut better = drop_at(7);
    better.supersedes = Some(first.id);
    let second = events::record(&db.pool, game.id, &better).await.unwrap();

    let now = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert_eq!(now.len(), 1, "one live row, not two");
    assert_eq!(now[0].id, second.id, "the correction is what is current");

    let then = events::known_at(&db.pool, game.id, before_correction)
        .await
        .unwrap();
    assert_eq!(then.len(), 1);
    assert_eq!(
        then[0].id, first.id,
        "a study run before the correction must still see what it saw"
    );
    db.cleanup().await;
}

/// Two titles run at once during a transition, and each has its own calendar.
#[tokio::test]
async fn events_are_kept_per_game() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let (next_game, _) = seed_second_game(&db.pool).await.unwrap();
    events::record(&db.pool, game.id, &drop_at(6))
        .await
        .unwrap();
    events::record(&db.pool, next_game, &drop_at(6))
        .await
        .unwrap();

    assert_eq!(
        events::known_at(&db.pool, game.id, Utc::now())
            .await
            .unwrap()
            .len(),
        1,
        "one title's calendar must not show another's"
    );
    db.cleanup().await;
}

/// A superseding row must belong to the same title as the row it replaces.
///
/// The supersedes id arrives from the caller, and the visibility check did not
/// constrain the superseding row's game. Together those let a row in one title
/// hide an event in another from every study, permanently and silently.
#[tokio::test]
async fn an_event_cannot_be_hidden_by_a_row_from_another_game() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let (other_game, _) = seed_second_game(&db.pool).await.unwrap();

    let target = events::record(&db.pool, game.id, &drop_at(6))
        .await
        .unwrap();

    // The other title tries to supersede this title's event.
    let mut intruder = drop_at(6);
    intruder.supersedes = Some(target.id);
    intruder.name = "From another game".into();
    let result = events::record(&db.pool, other_game, &intruder).await;

    assert!(
        result.is_err(),
        "superseding across titles must be refused outright"
    );

    let known = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        known.len(),
        1,
        "and the event must still be visible to its own title: {known:?}"
    );
    assert_eq!(known[0].id, target.id);
    db.cleanup().await;
}

/// The second door.
///
/// The write path refuses a cross-title supersede, so the read path's own guard
/// is never reached in normal use and would go untested. A row inserted directly
/// stands in for the ways one could arrive anyway: a restore, a manual fix, or a
/// future caller that skips `record`. Rule 1's history in this codebase is that a
/// guard installed at one door and not the other is the defect that ships.
#[tokio::test]
async fn a_cross_game_superseding_row_cannot_hide_an_event_even_if_it_exists() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let (other_game, _) = seed_second_game(&db.pool).await.unwrap();
    let target = events::record(&db.pool, game.id, &drop_at(6))
        .await
        .unwrap();

    // Straight into the table, bypassing the write guard entirely.
    sqlx::query(
        "INSERT INTO market_events (id, game_id, kind, name, occurred_at, supersedes)
         VALUES ($1, $2, 'content_drop', 'Smuggled', now(), $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(other_game.0)
    .bind(target.id)
    .execute(&db.pool)
    .await
    .expect("the database itself does not forbid this row");

    let known = events::known_at(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        known.len(),
        1,
        "a row from another title must not hide this title's event: {known:?}"
    );
    assert_eq!(known[0].id, target.id);
    db.cleanup().await;
}
