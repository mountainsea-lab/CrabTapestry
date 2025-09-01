use ms_tracing::{LogCache, LogEntry, LogQuery};
use tokio::sync::broadcast;
use warp::sse::Event;
use warp::{Filter, Rejection, Reply};

pub async fn sse_logs(tx: broadcast::Sender<LogEntry>) -> Result<impl Reply, Rejection> {
    let mut rx = tx.subscribe();
    let stream = async_stream::stream! {
        while let Ok(log) = rx.recv().await {
            let json = serde_json::to_string(&log).unwrap();
            yield Ok::<_, std::convert::Infallible>(Event::default().data(json));
        }
    };

    Ok(warp::sse::reply(warp::sse::keep_alive().stream(stream)))
}

pub async fn query_logs(params: LogQuery, cache: LogCache) -> Result<impl Reply, Rejection> {
    let logs = cache.read().await;
    let mut filtered: Vec<_> = logs
        .iter()
        .cloned()
        .filter(|entry| {
            params
                .level
                .as_ref()
                .map_or(true, |lvl| entry.level.eq_ignore_ascii_case(lvl))
                && params
                .keyword
                .as_ref()
                .map_or(true, |kw| entry.message.contains(kw))
        })
        .collect();

    filtered.reverse();

    let page = params.page.unwrap_or(1);
    let page_size = params.page_size.unwrap_or(100);
    let start = (page - 1) * page_size;
    let _end = start + page_size;

    let result = filtered
        .into_iter()
        .skip(start)
        .take(page_size)
        .collect::<Vec<_>>();

    Ok(warp::reply::json(&result))
}

pub fn with_tx(
    tx: broadcast::Sender<LogEntry>,
) -> impl Filter<Extract = (broadcast::Sender<LogEntry>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || tx.clone())
}

pub fn with_cache(
    cache: LogCache,
) -> impl Filter<Extract = (LogCache,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cache.clone())
}