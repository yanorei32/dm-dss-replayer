use chrono::NaiveDateTime;
use futures_util::{SinkExt, StreamExt};
use once_cell::sync::OnceCell;
use serde_json::Value;
use std::{ffi::OsStr, fs, net::SocketAddr, time::Duration};
use tap::Tap;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Debug, Clone)]
struct Event {
    json: Value,
    offset: Duration,
}

static EVENTS: OnceCell<Vec<Event>> = OnceCell::new();

#[tokio::main]
async fn main() {
    let json_dir = std::env::current_dir()
        .unwrap()
        .tap_mut(|dir| dir.push("json"));

    let mut events = vec![];

    let mut files: Vec<_> = fs::read_dir(&json_dir)
        .expect("Failed to open json dir")
        .collect();

    files.sort_by_cached_key(|v| v.as_ref().unwrap().path());

    let mut previous_event_at = None;

    for f in files {
        let f = f.unwrap();
        let path = f.path();
        let ext = path.extension().and_then(OsStr::to_str);

        if ext != Some("json") {
            continue;
        }

        let name = path.file_name().and_then(OsStr::to_str).unwrap();
        let dt = name
            .split('_')
            .skip(2)
            .next()
            .expect("Failed to parse json filename");

        let dt =
            NaiveDateTime::parse_from_str(&dt, "%Y%m%d%H%M%S").expect("Failed to parse DateTime");

        let content = fs::read_to_string(&path).unwrap();
        let v: Value = serde_json::from_str(&content).unwrap();

        let offset = match previous_event_at {
            Some(prev) => {
                let duration = dt - prev;
                chrono::Duration::to_std(&duration).unwrap()
            }
            None => Duration::from_secs(2),
        };

        previous_event_at = Some(dt);
        println!("{name} Offset: {}s", offset.as_secs());
        events.push(Event { json: v, offset });
    }

    EVENTS.set(events).unwrap();

    let listener = TcpListener::bind("0.0.0.0:1313").await.unwrap();

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, addr));
    }
}

async fn handle_connection(raw_stream: TcpStream, addr: SocketAddr) {
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");

    let (mut tx, _rx) = ws_stream.split();

    let events = EVENTS.get().unwrap();

    println!("Connected: {addr}");
    for e in events {
        println!("{addr} wait for {}s", e.offset.as_secs());
        tokio::time::sleep(e.offset).await;
        tx.send(Message::Text(e.json.to_string())).await.unwrap();
    }

    println!("All events are sent: {addr}");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
