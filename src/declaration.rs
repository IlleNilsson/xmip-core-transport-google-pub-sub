//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the REST API's
/// endpoint, `https://pubsub.googleapis.com` in the cloud. The bearer
/// token is the Location's `secret`, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "project",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The project the topic and the subscription belong to.",
            applies: Applies::Both,
        },
        Setting {
            name: "topic",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The topic a Location publishes to unless the target names another.",
            applies: Applies::Send,
        },
        Setting {
            name: "subscription",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The subscription a Location pulls from and acknowledges on.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// Its schemes, [`transport::Configured::SCHEMES`]: its name (ADR-0011,
/// amendment 2026-11-19).
pub const SCHEMES: &[&str] = &["google-pub-sub"];
