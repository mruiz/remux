//! Per-user connections to media trackers (MDBList, ...). Only token-based
//! trackers are offered: no OAuth flow is wired up yet.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use http::StatusCode;
use remux_macros::{delete, get, post};
use remux_sdks::remux::MediaTrackerDto;
use uuid::Uuid;

use crate::{
    AppState, IntoApiError, OptionExt,
    addons::media_tracker::{AuthFlow, MediaTrackerAddon, MediaTrackerCtx},
    db::{self, auth},
};
use axum_anyhow::ApiResult as Result;

/// Enabled token-based trackers, with their addon row name.
fn token_trackers(state: &AppState) -> Vec<(Uuid, String, Arc<dyn MediaTrackerAddon>)> {
    state
        .ctx
        .addons
        .list()
        .iter()
        .filter(|r| {
            r.row
                .enabled
        })
        .filter_map(|r| {
            let tracker = r
                .caps
                .media_tracker
                .clone()?;
            (tracker
                .capabilities()
                .auth_flow
                == AuthFlow::Token)
                .then(|| {
                    (
                        r.row
                            .id,
                        r.row
                            .name
                            .clone(),
                        tracker,
                    )
                })
        })
        .collect()
}

fn dto(
    addon_id: Uuid,
    name: String,
    tracker: &dyn MediaTrackerAddon,
    conn: Option<&db::UserMediaTracker>,
) -> MediaTrackerDto {
    MediaTrackerDto {
        addon_id,
        name,
        connect_fields: tracker
            .capabilities()
            .connect_fields,
        status: conn.map(|c| {
            c.status
                .to_string()
        }),
        last_error: conn.and_then(|c| {
            c.last_error
                .clone()
        }),
    }
}

#[get("/remux/users/{user_id}/mediatrackers")]
pub async fn list_user_media_trackers(
    State(state): State<AppState>,
    _session: auth::AuthSession,
    auth::TargetUser(user): auth::TargetUser,
) -> Result<impl IntoResponse> {
    let conns = db::UserMediaTracker::list_for_user(
        &state
            .ctx
            .db,
        user.id,
    )
    .await?;
    let out: Vec<MediaTrackerDto> = token_trackers(&state)
        .into_iter()
        .map(|(id, name, tracker)| {
            dto(
                id,
                name,
                tracker.as_ref(),
                conns
                    .iter()
                    .find(|c| c.addon_id == id),
            )
        })
        .collect();
    Ok(Json(out))
}

#[post("/remux/users/{user_id}/mediatrackers/{addon_id}")]
pub async fn connect_user_media_tracker(
    State(state): State<AppState>,
    _session: auth::AuthSession,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, addon_id)): Path<(Uuid, Uuid)>,
    Json(fields): Json<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let (_, name, tracker) = token_trackers(&state)
        .into_iter()
        .find(|(id, ..)| *id == addon_id)
        .context_not_found("Media tracker not found")?;
    let tctx = MediaTrackerCtx {
        config: Arc::new(
            state
                .ctx
                .config
                .clone(),
        ),
    };
    let creds = tracker
        .connect_with_token(&fields, &tctx)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            e.context_bad_request(&msg)
        })?;
    let conn = db::UserMediaTracker::new(
        user.id,
        addon_id,
        creds,
        tracker
            .capabilities()
            .default_event_filter,
    );
    conn.upsert(
        &state
            .ctx
            .db,
    )
    .await?;
    // A reconnect keeps the existing row and its id, so read it back.
    let stored = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        user.id,
        addon_id,
    )
    .await?
    .context_internal("Media tracker connection was not saved")?;
    // Clears an error left over from a previous connection; the key was just verified.
    db::UserMediaTracker::mark_success(
        &state
            .ctx
            .db,
        stored.id,
    )
    .await?;
    let stored = db::UserMediaTracker::get(
        &state
            .ctx
            .db,
        stored.id,
    )
    .await?;
    Ok(Json(dto(addon_id, name, tracker.as_ref(), stored.as_ref())))
}

#[delete("/remux/users/{user_id}/mediatrackers/{addon_id}")]
pub async fn disconnect_user_media_tracker(
    State(state): State<AppState>,
    _session: auth::AuthSession,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, addon_id)): Path<(Uuid, Uuid)>,
) -> Result<impl IntoResponse> {
    let Some(conn) = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        user.id,
        addon_id,
    )
    .await?
    else {
        return Ok(StatusCode::NO_CONTENT);
    };
    if let Some(tracker) = state
        .ctx
        .addons
        .media_tracker_for(addon_id)
    {
        let tctx = MediaTrackerCtx {
            config: Arc::new(
                state
                    .ctx
                    .config
                    .clone(),
            ),
        };
        // Best effort: disconnecting must succeed locally even when the provider is down.
        let _ = tracker
            .disconnect(&conn.credentials, &tctx)
            .await;
    }
    db::UserMediaTracker::delete(
        &state
            .ctx
            .db,
        conn.id,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use crate::{
        addons::{
            AddonKind,
            media_tracker::{
                MediaTrackerAddon, MediaTrackerCapabilities, MediaTrackerCredentials,
                MediaTrackerCtx, MediaTrackerError, MediaTrackerEvent,
                MediaTrackerEventKind, MediaTrackerResult, MediaTrackerTarget,
            },
        },
        db,
        integration_test::{
            auth_header_with_token, authenticated_server, register_media_tracker,
        },
    };
    use async_trait::async_trait;
    use http::header::{AUTHORIZATION, HeaderValue};
    use serde_json::json;
    use std::sync::Arc;

    /// Accepts only the key "good".
    struct KeyTracker;

    impl AddonKind for KeyTracker {
        fn id(&self) -> &'static str {
            "scripted"
        }
    }

    #[async_trait]
    impl MediaTrackerAddon for KeyTracker {
        fn capabilities(&self) -> MediaTrackerCapabilities {
            MediaTrackerCapabilities {
                supported_events: vec![MediaTrackerEventKind::MarkPlayed],
                default_event_filter: vec![MediaTrackerEventKind::MarkPlayed],
                ..Default::default()
            }
        }

        async fn connect_with_token(
            &self,
            fields: &serde_json::Value,
            _ctx: &MediaTrackerCtx,
        ) -> MediaTrackerResult<MediaTrackerCredentials> {
            if fields["apikey"] == "good" {
                Ok(MediaTrackerCredentials::new(fields.clone()))
            } else {
                Err(MediaTrackerError::reauth("bad key"))
            }
        }

        async fn on_event(
            &self,
            _event: &MediaTrackerEvent,
            _target: &MediaTrackerTarget,
            _creds: &MediaTrackerCredentials,
            _ctx: &MediaTrackerCtx,
        ) -> MediaTrackerResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_user_connects_and_disconnects_a_token_tracker() {
        let (server, guard, token) = authenticated_server().await;
        let ctx = &guard.0;
        let addon = register_media_tracker(ctx, "Keyed", Arc::new(KeyTracker)).await;
        let user = db::User::get_by_username(&ctx.db, "test")
            .await
            .unwrap()
            .unwrap();
        let hdr = HeaderValue::from_str(&auth_header_with_token(&token)).unwrap();
        let base = format!("/remux/users/{}/mediatrackers", user.id);
        let one = format!("{base}/{}", addon.id);

        let list = server
            .get(&base)
            .add_header(AUTHORIZATION, hdr.clone())
            .await
            .json::<serde_json::Value>();
        let entry = list
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["addonId"] == json!(addon.id))
            .unwrap();
        assert!(
            entry
                .get("status")
                .is_none()
        );

        let bad = server
            .post(&one)
            .add_header(AUTHORIZATION, hdr.clone())
            .json(&json!({ "apikey": "nope" }))
            .expect_failure()
            .await;
        assert_eq!(bad.status_code(), http::StatusCode::BAD_REQUEST);

        let ok = server
            .post(&one)
            .add_header(AUTHORIZATION, hdr.clone())
            .json(&json!({ "apikey": "good" }))
            .await;
        assert_eq!(ok.status_code(), http::StatusCode::OK);
        assert_eq!(ok.json::<serde_json::Value>()["status"], "connected");
        let stored =
            db::UserMediaTracker::get_for_user_and_addon(&ctx.db, user.id, addon.id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            stored.event_filters,
            vec![MediaTrackerEventKind::MarkPlayed]
        );
        assert_eq!(
            stored
                .credentials
                .get_str("apikey"),
            Some("good")
        );

        let gone = server
            .delete(&one)
            .add_header(AUTHORIZATION, hdr)
            .await;
        assert_eq!(gone.status_code(), http::StatusCode::NO_CONTENT);
        assert!(
            db::UserMediaTracker::get_for_user_and_addon(&ctx.db, user.id, addon.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
