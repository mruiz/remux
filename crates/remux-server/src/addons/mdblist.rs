//! MDBList media tracker: pushes each user's playback, watched state and
//! ratings to their MDBList account. Users connect with their own API key.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use super::{
    AddonCapabilities, AddonKind, AddonMetadata, AddonPreset, AddonPresetRegistration,
    MediaKind, ResourceType,
    media_tracker::{
        AuthFlow, MediaTrackerAddon, MediaTrackerCapabilities, MediaTrackerCredentials,
        MediaTrackerCtx, MediaTrackerError, MediaTrackerEvent, MediaTrackerEventKind,
        MediaTrackerResult, MediaTrackerTarget, SyncDirection,
    },
};
use crate::db;
use remux_sdks::remux::{AddonOption, AddonOptionType};

const BASE: &str = "https://api.mdblist.com";
const TICKS_PER_SECOND: f64 = 10_000_000.0;

pub struct MdbListPreset;

impl AddonPreset for MdbListPreset {
    fn id(&self) -> &'static str {
        "mdblist"
    }

    fn metadata(&self) -> AddonMetadata {
        AddonMetadata {
            id: "mdblist".to_string(),
            display_name: "MDBList".to_string(),
            description: "Sync watch history, playback and ratings to MDBList. \
                 Each user connects with their own API key."
                .to_string(),
            icon: None,
            supported_resources: vec![AddonMetadata::simple_resource(
                ResourceType::Tracking,
            )],
            supported_types: vec![
                MediaKind::Movie,
                MediaKind::Series,
                MediaKind::Episode,
            ],
            supported_resources_user: vec![],
            supported_types_user: vec![],
            options: vec![],
        }
    }

    fn from_cfg(
        &self,
        _addon_id: Uuid,
        _cfg: &serde_json::Value,
        _config: &crate::Config,
    ) -> Result<AddonCapabilities> {
        let addon = Arc::new(MdbListAddon {
            client: super::make_http_client(),
        });
        Ok(AddonCapabilities {
            kind: Some(addon.clone()),
            media_tracker: Some(addon),
            ..Default::default()
        })
    }
}

inventory::submit! {
    AddonPresetRegistration(|| Box::new(MdbListPreset))
}

pub struct MdbListAddon {
    client: reqwest::Client,
}

impl MdbListAddon {
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        apikey: &str,
        body: Option<Value>,
    ) -> MediaTrackerResult<()> {
        let url = reqwest::Url::parse_with_params(
            &format!("{BASE}{path}"),
            &[("apikey", apikey)],
        )
        .map_err(|e| MediaTrackerError::permanent(format!("mdblist: {e}")))?;
        let mut req = self
            .client
            .request(method, url);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| MediaTrackerError::retryable(format!("mdblist: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| {
                v.to_str()
                    .ok()
            })
            .and_then(|v| {
                v.parse::<u64>()
                    .ok()
            })
            .map(Duration::from_secs);
        let text = resp
            .text()
            .await
            .unwrap_or_default();
        Err(classify(status, retry_after, &text))
    }
}

fn classify(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    body: &str,
) -> MediaTrackerError {
    let message = format!("mdblist returned {status}: {body}");
    match status.as_u16() {
        401 | 403 => MediaTrackerError::reauth(message),
        429 => match retry_after {
            Some(after) => MediaTrackerError::retry_after(message, after),
            None => MediaTrackerError::retryable(message),
        },
        s if s >= 500 => MediaTrackerError::retryable(message),
        _ => MediaTrackerError::permanent(message),
    }
}

fn apikey(creds: &MediaTrackerCredentials) -> MediaTrackerResult<&str> {
    creds
        .get_str("apikey")
        .ok_or_else(|| MediaTrackerError::reauth("missing MDBList API key"))
}

/// MDBList accepts imdb, tmdb and tvdb; Kitsu and the rest mean nothing to it.
fn ids_json(ids: &db::ExternalIds) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(imdb) = &ids.imdb {
        out.insert("imdb".into(), json!(imdb.as_ref()));
    }
    if let Some(tmdb) = ids.tmdb {
        out.insert("tmdb".into(), json!(tmdb));
    }
    if let Some(tvdb) = ids.tvdb {
        out.insert("tvdb".into(), json!(tvdb));
    }
    Value::Object(out)
}

/// Body for the `/sync/*` endpoints, with `leaf` merged into the object that
/// names the item itself (the episode, for an episode). `None` for kinds
/// MDBList does not track.
fn sync_body(target: &MediaTrackerTarget, leaf: Value) -> Option<Value> {
    let mut item = leaf;
    match target.kind {
        db::MediaKind::Movie | db::MediaKind::Series => {
            item["ids"] = ids_json(&target.ids);
            let key = if target.kind == db::MediaKind::Movie {
                "movies"
            } else {
                "shows"
            };
            Some(json!({ key: [item] }))
        }
        db::MediaKind::Episode => {
            let series = target
                .series
                .as_ref()?;
            item["number"] = json!(target.episode?);
            Some(json!({ "shows": [{
                "ids": ids_json(&series.ids),
                "seasons": [{ "number": target.season?, "episodes": [item] }],
            }]}))
        }
        _ => None,
    }
}

/// Body for the `/scrobble/*` endpoints. `None` for kinds MDBList cannot
/// scrobble.
fn scrobble_body(target: &MediaTrackerTarget, progress: f64) -> Option<Value> {
    let mut body = match target.kind {
        db::MediaKind::Movie => json!({ "movie": { "ids": ids_json(&target.ids) } }),
        db::MediaKind::Episode => {
            let series = target
                .series
                .as_ref()?;
            json!({ "show": {
                "ids": ids_json(&series.ids),
                "season": target.season?,
                "episode": target.episode?,
            }})
        }
        _ => return None,
    };
    body["progress"] = json!(progress);
    body["app_name"] = json!("Remux");
    Some(body)
}

/// 0-100, or 0 when the runtime is unknown.
fn progress(target: &MediaTrackerTarget, position_ticks: i64) -> f64 {
    match target.runtime {
        Some(secs) if secs > 0 => {
            (position_ticks as f64 / TICKS_PER_SECOND / secs as f64 * 100.0)
                .clamp(0.0, 100.0)
        }
        _ => 0.0,
    }
}

/// MDBList takes 1-10 in 0.5 steps.
fn mdblist_rating(rating: f32) -> f64 {
    ((rating as f64 * 2.0).round() / 2.0).clamp(1.0, 10.0)
}

#[async_trait]
impl AddonKind for MdbListAddon {
    fn id(&self) -> &'static str {
        "mdblist"
    }
}

#[async_trait]
impl MediaTrackerAddon for MdbListAddon {
    fn capabilities(&self) -> MediaTrackerCapabilities {
        use MediaTrackerEventKind::*;
        MediaTrackerCapabilities {
            auth_flow: AuthFlow::Token,
            connect_fields: vec![AddonOption {
                id: "apikey".into(),
                name: "MDBList API key".into(),
                description: Some("Found at mdblist.com/preferences under API.".into()),
                required: true,
                default: None,
                kind: AddonOptionType::Password,
            }],
            supported_events: vec![
                PlaybackStart,
                PlaybackProgress,
                PlaybackStop,
                MarkPlayed,
                MarkUnplayed,
                Rating,
            ],
            // Progress is left out: every event is a request against a
            // 1,000/day free-tier quota.
            default_event_filter: vec![
                PlaybackStart,
                PlaybackStop,
                MarkPlayed,
                MarkUnplayed,
                Rating,
            ],
            watch_state_sync: SyncDirection::Push,
            ratings: SyncDirection::Push,
            ..Default::default()
        }
    }

    async fn connect_with_token(
        &self,
        fields: &serde_json::Value,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<MediaTrackerCredentials> {
        let key = fields
            .get("apikey")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .ok_or_else(|| {
                MediaTrackerError::permanent("an MDBList API key is required")
            })?;
        let creds = MediaTrackerCredentials::new(json!({ "apikey": key }));
        self.verify(&creds, ctx)
            .await?;
        Ok(creds)
    }

    async fn verify(
        &self,
        creds: &MediaTrackerCredentials,
        _ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<()> {
        self.call(reqwest::Method::GET, "/user", apikey(creds)?, None)
            .await
    }

    async fn on_event(
        &self,
        event: &MediaTrackerEvent,
        target: &MediaTrackerTarget,
        creds: &MediaTrackerCredentials,
        _ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<()> {
        let (path, body) = match event {
            MediaTrackerEvent::PlaybackStart { position_ticks } => (
                "/scrobble/start",
                scrobble_body(target, progress(target, *position_ticks)),
            ),
            MediaTrackerEvent::PlaybackProgress {
                position_ticks,
                is_paused: true,
            } => (
                "/scrobble/pause",
                scrobble_body(target, progress(target, *position_ticks)),
            ),
            // MDBList keeps a started session alive on its own; only pauses
            // change its state.
            MediaTrackerEvent::PlaybackProgress { .. } => return Ok(()),
            MediaTrackerEvent::PlaybackStop {
                position_ticks,
                played,
            } => {
                // MDBList marks watched from 80%; `played` is remux's own call.
                let p = if *played {
                    100.0
                } else {
                    progress(target, *position_ticks).min(79.0)
                };
                ("/scrobble/stop", scrobble_body(target, p))
            }
            MediaTrackerEvent::MarkPlayed => (
                "/sync/watched",
                // Without a date MDBList skips items already watched once.
                sync_body(
                    target,
                    json!({ "watched_at": chrono::Utc::now().to_rfc3339() }),
                ),
            ),
            MediaTrackerEvent::MarkUnplayed => {
                ("/sync/watched/remove", sync_body(target, json!({})))
            }
            MediaTrackerEvent::Rating { rating: Some(r) } => (
                "/sync/ratings",
                sync_body(target, json!({ "rating": mdblist_rating(*r) })),
            ),
            MediaTrackerEvent::Rating { rating: None } => {
                ("/sync/ratings/remove", sync_body(target, json!({})))
            }
            MediaTrackerEvent::MarkFavorite | MediaTrackerEvent::UnmarkFavorite => {
                return Err(MediaTrackerError::unsupported("favorites"));
            }
        };
        let Some(body) = body else {
            return Ok(());
        };
        self.call(reqwest::Method::POST, path, apikey(creds)?, Some(body))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(tmdb: i64) -> db::ExternalIds {
        db::ExternalIds {
            tmdb: Some(tmdb),
            imdb: db::NonEmptyString::try_new("tt1".to_string()).ok(),
            ..Default::default()
        }
    }

    fn movie() -> MediaTrackerTarget {
        MediaTrackerTarget {
            kind: db::MediaKind::Movie,
            title: "Heat".into(),
            year: Some(1995),
            ids: ids(949),
            series: None,
            season: None,
            episode: None,
            runtime: Some(100),
        }
    }

    fn episode() -> MediaTrackerTarget {
        let mut series = movie();
        series.kind = db::MediaKind::Series;
        series.ids = ids(1396);
        MediaTrackerTarget {
            kind: db::MediaKind::Episode,
            title: "Pilot".into(),
            year: None,
            ids: db::ExternalIds::default(),
            series: Some(Box::new(series)),
            season: Some(1),
            episode: Some(2),
            runtime: None,
        }
    }

    #[test]
    fn a_movie_syncs_under_movies_with_its_ids() {
        assert_eq!(
            sync_body(&movie(), json!({ "rating": 8.0 })).unwrap(),
            json!({ "movies": [{ "rating": 8.0, "ids": { "imdb": "tt1", "tmdb": 949 } }] })
        );
    }

    #[test]
    fn an_episode_syncs_nested_under_its_show() {
        assert_eq!(
            sync_body(&episode(), json!({ "rating": 9.0 })).unwrap(),
            json!({ "shows": [{
                "ids": { "imdb": "tt1", "tmdb": 1396 },
                "seasons": [{ "number": 1, "episodes": [{ "rating": 9.0, "number": 2 }] }],
            }]})
        );
        assert_eq!(
            scrobble_body(&episode(), 0.0).unwrap()["show"],
            json!({ "ids": { "imdb": "tt1", "tmdb": 1396 }, "season": 1, "episode": 2 })
        );
    }

    #[test]
    fn an_episode_without_its_show_is_skipped() {
        let mut ep = episode();
        ep.series = None;
        assert!(sync_body(&ep, json!({})).is_none());
        assert!(scrobble_body(&ep, 0.0).is_none());
    }

    #[test]
    fn progress_is_a_clamped_percentage_of_runtime() {
        let t = movie();
        assert_eq!(progress(&t, 50 * 10_000_000), 50.0);
        assert_eq!(progress(&t, 500 * 10_000_000), 100.0);
        assert_eq!(progress(&episode(), 50 * 10_000_000), 0.0);
    }

    #[test]
    fn ratings_round_to_half_steps_within_range() {
        assert_eq!(mdblist_rating(7.3), 7.5);
        assert_eq!(mdblist_rating(0.0), 1.0);
        assert_eq!(mdblist_rating(10.0), 10.0);
    }

    #[test]
    fn auth_failures_ask_for_a_reconnect() {
        let s = reqwest::StatusCode::from_u16;
        assert!(classify(s(401).unwrap(), None, "").requires_reauth());
        assert!(classify(s(429).unwrap(), None, "").is_retryable());
        assert!(classify(s(503).unwrap(), None, "").is_retryable());
        assert!(!classify(s(400).unwrap(), None, "").is_retryable());
    }
}
