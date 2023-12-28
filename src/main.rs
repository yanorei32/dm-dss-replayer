use chrono::NaiveDateTime;
use futures_util::{SinkExt, StreamExt};
use once_cell::sync::OnceCell;
use std::{ffi::OsStr, fs, net::SocketAddr, time::Duration};
use tap::Tap;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Listen address e.g. 0.0.0.0:1313
    #[arg(long, verbatim_doc_comment)]
    listen: SocketAddr,

    /// First event schedule at
    #[arg(long, default_value_t = 3)]
    first_event_at: u64,
}

#[derive(Debug, Clone)]
struct Event {
    content: String,
    filename: String,
    offset: Duration,
}

static EVENTS: OnceCell<Vec<Event>> = OnceCell::new();

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let json_dir = std::env::current_dir()
        .unwrap()
        .tap_mut(|dir| dir.push("json"));

    let mut files: Vec<_> = fs::read_dir(&json_dir)
        .expect("Failed to open json dir")
        .map(|f| f.unwrap())
        .filter(|v| v.path().extension().is_some_and(|v| v == "json"))
        .collect();

    files.sort_by_cached_key(|v| v.path());

    let events: Vec<_> = files
        .iter()
        .scan(None, |prev_dt, f| {
            let path = f.path();
            let filename = path.file_name().and_then(OsStr::to_str).unwrap();

            let dt = filename
                .split('_')
                .nth(2)
                .expect("Failed to parse json filename");

            let dt = NaiveDateTime::parse_from_str(dt, "%Y%m%d%H%M%S")
                .expect("Failed to parse DateTime");

            let offset = match *prev_dt {
                Some(prev) => chrono::Duration::to_std(&(dt - prev)).unwrap(),
                None => Duration::from_secs(args.first_event_at),
            };

            *prev_dt = Some(dt);

            Some(Event {
                content: fs::read_to_string(&path).unwrap(),
                filename: filename.to_string(),
                offset,
            })
        })
        .collect();

    println!("[ {} json(s) loaded ]", events.len());
    events
        .iter()
        .for_each(|v| println!(" `- +{:3}s: {}", v.offset.as_secs(), v.filename));

    EVENTS.set(events).unwrap();
    println!();

    let listener = TcpListener::bind(args.listen).await.unwrap();
    println!("Listen at {}", args.listen);

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
        println!("Send {} to {addr} after {} seconds", e.filename, e.offset.as_secs());
        tokio::time::sleep(e.offset).await;
        tx.send(Message::Text(e.content.to_owned())).await.unwrap();
    }

    println!("All events are sent for {addr}");

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
