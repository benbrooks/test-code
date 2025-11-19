use anyhow::Result;
use axum::{
    extract::State,
    response::Html,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;

const MAX_POSTS: usize = 250;
const PORT: u16 = 2287;
const STORAGE_FILE: &str = "posts.json";

// Linux and AI-related keywords (case-insensitive matching)
const KEYWORDS: &[&str] = &[
    // Linux distributions
    "linux",
    "ubuntu",
    "debian",
    "fedora",
    "arch",
    "manjaro",
    "mint",
    "opensuse",
    "gentoo",
    "redhat",
    "centos",
    "rocky",
    "alma",
    "kali",
    "parrot",
    "elementary",
    "zorin",
    "pop!_os",
    "endeavouros",
    "nixos",
    // AI companies and products
    "claude",
    "anthropic",
    "chatgpt",
    "openai",
    "gemini",
    "mistral",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Post {
    uri: String,
    cid: String,
    author: String,
    author_handle: String,
    text: String,
    created_at: DateTime<Utc>,
    images: Vec<String>,
    links: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct JetstreamMessage {
    did: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    commit: Option<JetstreamCommit>,
}

#[derive(Debug, Deserialize)]
struct JetstreamCommit {
    record: Option<serde_json::Value>,
    #[serde(rename = "type")]
    commit_type: Option<String>,
    rev: Option<String>,
}

type SharedPosts = Arc<RwLock<Vec<Post>>>;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting Bluesky Linux & AI Monitor");

    // Load existing posts from disk
    let posts = load_posts().await;
    let shared_posts = Arc::new(RwLock::new(posts));

    // Spawn firehose listener
    let posts_clone = shared_posts.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_to_firehose(posts_clone).await {
            eprintln!("Firehose error: {}", e);
        }
    });

    // Start web server
    println!("🌐 Web interface available at http://localhost:{}", PORT);
    start_web_server(shared_posts).await?;

    Ok(())
}

async fn listen_to_firehose(posts: SharedPosts) -> Result<()> {
    println!("🔌 Connecting to Bluesky firehose...");

    loop {
        match connect_async("wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post").await {
            Ok((ws_stream, _)) => {
                println!("✅ Connected to firehose");
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Err(e) = process_message(&text, &posts).await {
                                eprintln!("Error processing message: {}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to firehose: {}", e);
            }
        }

        println!("⏳ Reconnecting in 5 seconds...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn process_message(text: &str, posts: &SharedPosts) -> Result<()> {
    let msg: JetstreamMessage = serde_json::from_str(text)?;

    // Check if this is a post creation
    if msg.msg_type.as_deref() != Some("commit") {
        return Ok(());
    }

    let Some(commit) = msg.commit else {
        return Ok(());
    };

    if commit.commit_type.as_deref() != Some("create") {
        return Ok(());
    }

    let Some(record) = commit.record else {
        return Ok(());
    };

    // Check if it's a post with text
    let Some(text_content) = record.get("text").and_then(|v| v.as_str()) else {
        return Ok(());
    };

    // Check if text contains any of our keywords (case-insensitive)
    let text_lower = text_content.to_lowercase();
    if !KEYWORDS.iter().any(|keyword| text_lower.contains(keyword)) {
        return Ok(());
    }

    // Extract post data
    let author = msg.did.unwrap_or_default();
    let created_at = record
        .get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    // Extract images
    let mut images = Vec::new();
    if let Some(embed) = record.get("embed") {
        if let Some(imgs) = embed.get("images").and_then(|v| v.as_array()) {
            for img in imgs {
                if let Some(alt) = img.get("alt").and_then(|v| v.as_str()) {
                    images.push(alt.to_string());
                }
            }
        }
    }

    // Extract links (facets)
    let mut links = Vec::new();
    if let Some(facets) = record.get("facets").and_then(|v| v.as_array()) {
        for facet in facets {
            if let Some(features) = facet.get("features").and_then(|v| v.as_array()) {
                for feature in features {
                    if let Some(uri) = feature.get("uri").and_then(|v| v.as_str()) {
                        links.push(uri.to_string());
                    }
                }
            }
        }
    }

    // Get author handle (did by default, we can resolve it later if needed)
    let author_handle = author.clone();

    let post = Post {
        uri: format!("at://{}/app.bsky.feed.post/{}", author, record.get("$id").and_then(|v| v.as_str()).unwrap_or("unknown")),
        cid: commit.rev.unwrap_or_default(),
        author: author.clone(),
        author_handle,
        text: text_content.to_string(),
        created_at,
        images,
        links,
    };

    println!("📝 New post from {}: {}", post.author_handle, &post.text[..post.text.len().min(50)]);

    // Add to posts
    let mut posts_guard = posts.write().await;
    posts_guard.insert(0, post);

    // Keep only last MAX_POSTS
    if posts_guard.len() > MAX_POSTS {
        posts_guard.truncate(MAX_POSTS);
    }

    // Save to disk
    drop(posts_guard);
    save_posts(&posts).await?;

    Ok(())
}

async fn load_posts() -> Vec<Post> {
    match tokio::fs::read_to_string(STORAGE_FILE).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn save_posts(posts: &SharedPosts) -> Result<()> {
    let posts_guard = posts.read().await;
    let json = serde_json::to_string_pretty(&*posts_guard)?;
    tokio::fs::write(STORAGE_FILE, json).await?;
    Ok(())
}

async fn start_web_server(posts: SharedPosts) -> Result<()> {
    let app = Router::new()
        .route("/", get(index_handler))
        .with_state(posts);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", PORT)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn index_handler(State(posts): State<SharedPosts>) -> Html<String> {
    let posts_guard = posts.read().await;

    let posts_html: String = posts_guard
        .iter()
        .map(|post| {
            let images_html: String = post
                .images
                .iter()
                .map(|img| format!(r#"<div class="image-placeholder">🖼️ Image: {}</div>"#, img))
                .collect();

            let links_html: String = post
                .links
                .iter()
                .map(|link| format!(r#"<a href="{}" target="_blank" class="link">🔗 {}</a>"#, link, link))
                .collect();

            let bsky_url = format!("https://bsky.app/profile/{}/post/{}",
                post.author_handle,
                post.uri.split('/').last().unwrap_or("")
            );

            format!(
                r#"
                <div class="post">
                    <div class="post-header">
                        <strong>{}</strong>
                        <span class="timestamp">{}</span>
                    </div>
                    <div class="post-text">{}</div>
                    {}
                    {}
                    <div class="post-footer">
                        <a href="{}" target="_blank" class="view-on-bsky">View on Bluesky →</a>
                    </div>
                </div>
                "#,
                post.author_handle,
                post.created_at.format("%Y-%m-%d %H:%M:%S UTC"),
                post.text,
                images_html,
                links_html,
                bsky_url
            )
        })
        .collect();

    let count = posts_guard.len();

    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="10">
    <title>Bluesky Linux & AI Monitor</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f;
            color: #e0e0e0;
            line-height: 1.6;
        }}

        .header {{
            background: #1a1a1a;
            border-bottom: 2px solid #00ff00;
            padding: 1.5rem;
            position: sticky;
            top: 0;
            z-index: 100;
        }}

        .header h1 {{
            color: #00ff00;
            font-size: 1.5rem;
            font-weight: 600;
        }}

        .header p {{
            color: #888;
            font-size: 0.9rem;
            margin-top: 0.25rem;
        }}

        .container {{
            max-width: 800px;
            margin: 0 auto;
            padding: 2rem 1rem;
        }}

        .stats {{
            background: #1a1a1a;
            padding: 1rem;
            border-radius: 4px;
            margin-bottom: 2rem;
            border-left: 3px solid #00ff00;
        }}

        .post {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 4px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            transition: border-color 0.2s;
        }}

        .post:hover {{
            border-color: #00ff00;
        }}

        .post-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
            color: #00ff00;
        }}

        .timestamp {{
            font-size: 0.85rem;
            color: #666;
        }}

        .post-text {{
            color: #e0e0e0;
            margin-bottom: 1rem;
            white-space: pre-wrap;
            word-wrap: break-word;
        }}

        .image-placeholder {{
            background: #0a0a0a;
            border: 1px dashed #333;
            padding: 0.75rem;
            margin: 0.5rem 0;
            border-radius: 4px;
            font-size: 0.9rem;
            color: #888;
        }}

        .link {{
            display: inline-block;
            color: #00aaff;
            text-decoration: none;
            margin: 0.25rem 0.5rem 0.25rem 0;
            font-size: 0.9rem;
        }}

        .link:hover {{
            text-decoration: underline;
        }}

        .post-footer {{
            margin-top: 1rem;
            padding-top: 0.75rem;
            border-top: 1px solid #333;
        }}

        .view-on-bsky {{
            color: #00ff00;
            text-decoration: none;
            font-size: 0.9rem;
        }}

        .view-on-bsky:hover {{
            text-decoration: underline;
        }}

        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🐧 Bluesky Linux & AI Monitor</h1>
        <p>Real-time feed of Linux and AI-related posts • Auto-refreshes every 10s</p>
    </div>

    <div class="container">
        <div class="stats">
            📊 Tracking {} posts mentioning Linux and AI topics
        </div>

        {}

        {}
    </div>
</body>
</html>"#,
        count,
        if count == 0 {
            r#"<div class="empty-state">Waiting for Linux and AI-related posts...</div>"#.to_string()
        } else {
            String::new()
        },
        posts_html
    ))
}
