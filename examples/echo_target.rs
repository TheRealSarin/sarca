use std::time::Duration;

use sarca::target::{Call, LocalRule, Target, TargetConfig, Trigger};
use sarca::wire::{ActionSpec, ErrorCode};

#[tokio::main]
async fn main() {
    let url = std::env::var("SARCA_URL").unwrap_or_else(|_| "tcp://127.0.0.1:7878".into());
    let name = std::env::args().nth(1).unwrap_or_else(|| "echo".into());
    let mut config = TargetConfig::new(
        url,
        name,
        vec![ActionSpec::new("echo"), ActionSpec::new("count")],
    );
    config.token = std::env::var("SARCA_TOKEN").ok();
    config.groups = vec!["examples".into()];
    config.rules = vec![LocalRule {
        trigger: Trigger::Every(Duration::from_secs(10)),
        action: "tick".into(),
        payload: Vec::new(),
    }];

    let target = Target::new(config);
    let events = target.handle();
    let handler = move |mut call: Call| {
        let events = events.clone();
        async move {
            match call.action() {
                "echo" => {
                    let payload = call.payload().to_vec();
                    let _ = call.reply(payload).await;
                }
                "count" => {
                    let n = String::from_utf8_lossy(call.payload())
                        .trim()
                        .parse()
                        .unwrap_or(3u32);
                    for i in 1..=n {
                        if call.stream_item(format!("{i}\n")).await.is_err() {
                            return;
                        }
                    }
                    let _ = call.end().await;
                }
                "tick" => {
                    let _ = events.emit("tick", b"alive".to_vec()).await;
                }
                other => {
                    let message = format!("unhandled action {other}");
                    let _ = call.fail(ErrorCode::UnknownAction, message, false).await;
                }
            }
        }
    };
    if let Err(err) = target.run(handler).await {
        eprintln!("target stopped: {err}");
        std::process::exit(1);
    }
}
