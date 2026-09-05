use super::LocalThreadStore;
use super::list_threads::list_threads;
use crate::ListThreadsParams;
use crate::SearchThreadsParams;
use crate::StoredThreadSearchResult;
use crate::ThreadSearchPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[path = "search_threads_tests.rs"]
mod tests;

pub(super) async fn search_threads(
    store: &LocalThreadStore,
    params: SearchThreadsParams,
) -> ThreadStoreResult<ThreadSearchPage> {
    if params.search_term.is_empty() || params.page_size == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/search requires search_term and page_size greater than zero"
                .to_string(),
        });
    }
    let page = list_threads(
        store,
        ListThreadsParams {
            page_size: params.page_size,
            cursor: params.cursor,
            sort_key: params.sort_key,
            sort_direction: params.sort_direction,
            allowed_sources: params.allowed_sources,
            model_providers: None,
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: params.archived,
            search_term: Some(params.search_term),
            relation_filter: None,
            use_state_db_only: true,
        },
    )
    .await?;

    Ok(ThreadSearchPage {
        items: page
            .items
            .into_iter()
            .map(|thread| StoredThreadSearchResult {
                snippet: thread.preview.clone(),
                thread,
            })
            .collect(),
        next_cursor: page.next_cursor,
    })
}
