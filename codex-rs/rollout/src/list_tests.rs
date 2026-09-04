use std::fs;
use std::io;

use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::UserMessageEvent;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::HEAD_RECORD_LIMIT;
use super::read_head_summary;
use crate::RolloutItem;
use crate::RolloutLine;

#[tokio::test]
async fn excluded_sources_skip_rollout_tail() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let path = home.path().join("rollout.jsonl");
    let meta = RolloutLine {
        timestamp: "2025-01-02T10:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                source: SessionSource::VSCode,
                ..Default::default()
            },
            git: None,
        }),
    };
    let mut contents = serde_json::to_vec(&meta)?;
    contents.push(b'\n');
    let head_len = contents.len();
    contents.extend_from_slice(b"\xff\n");
    fs::write(&path, &contents)?;

    let excluded = read_head_summary(&path, HEAD_RECORD_LIMIT, &[SessionSource::Cli]).await?;
    assert!(excluded.saw_session_meta);
    assert_eq!(excluded.source, Some(SessionSource::VSCode));
    assert_eq!(excluded.preview, None);
    for sources in [&[SessionSource::VSCode][..], &[][..]] {
        let result = read_head_summary(&path, HEAD_RECORD_LIMIT, sources).await;
        assert_eq!(
            result.err().map(|err| err.kind()),
            Some(io::ErrorKind::InvalidData)
        );
    }

    contents.truncate(head_len);
    contents.extend(serde_json::to_vec(&RolloutLine {
        timestamp: "2025-01-02T10:00:01Z".to_string(),
        ordinal: None,
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "matching preview".to_string(),
            ..Default::default()
        })),
    })?);
    contents.push(b'\n');
    fs::write(&path, &contents)?;
    for sources in [&[SessionSource::VSCode][..], &[][..]] {
        let matched = read_head_summary(&path, HEAD_RECORD_LIMIT, sources).await?;
        assert_eq!(matched.preview.as_deref(), Some("matching preview"));
        assert_eq!(
            matched.first_user_message.as_deref(),
            Some("matching preview")
        );
    }
    Ok(())
}
