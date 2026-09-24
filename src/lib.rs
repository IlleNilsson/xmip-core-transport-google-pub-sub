#![forbid(unsafe_code)]

//! Streams that arrive as messages on a Pub/Sub subscription. One message
//! is one Stream, its id kept beside it.
//!
//! Pub/Sub is the message bus of every organisation that lives in Google
//! Cloud: a topic, and subscriptions that keep what is published to it
//! until each reader acknowledges. Its REST API is three calls: publish
//! to a topic, pull from a subscription, acknowledge what was pulled. A
//! Receive Location pulls, hands each message on as a Stream and
//! acknowledges it once it is; a Send Location publishes a Stream as one
//! message. Both present a bearer token over plain HTTP/1.1 on a socket —
//! `https://` with the `tls` feature, which is the http technology's TLS
//! (ADR-0033). Obtaining the token is outside: a Location is configured
//! with it.
//!
//! ```text
//! client.rs    Xmip's side: publish, pull, acknowledge, JSON read by serde
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, HTTP itself and the judgement of an answer come from the
//! http technology (ADR-0044).
//!
//! A message is bytes, base64 on the wire, ten mebibytes at most:
//! [`ceiling`]. A message has data or an attribute, and a Stream is data,
//! so an empty Stream is what the service does not carry: [`refusal`]
//! says so before a request is formed rather than after the service
//! answers `INVALID_ARGUMENT`.
//!
//! A subscription is not an artefact anyone claims: a pulled message is
//! in flight until it is acknowledged, which is the subscription's own
//! claim, so [`Transport::claims`] answers `None`. The origin URI is
//! `pubsub://projects/<p>/subscriptions/<s>#<id>` for what was pulled and
//! `pubsub://projects/<p>/topics/<t>#<id>` for what the far end took. A
//! send target is a topic — `projects/<p>/topics/<t>`, or a name alone in
//! this transport's project — or empty for its own.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority and takes the one publish.

pub mod client;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, MAX_MESSAGES, Received};
use http::endpoint;
pub use session::{Event, Session};
use transport::ceiling;
use transport::error::{Result, TransportError, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

/// The largest message Pub/Sub carries: ten mebibytes, the number its own
/// refusal names.
#[must_use]
pub const fn ceiling() -> usize {
    10 * 1024 * 1024
}

/// Why `bytes` cannot travel as a message, or `None` where they can: a
/// message has data or an attribute, and an empty Stream has neither.
#[must_use]
pub fn refusal(bytes: &[u8]) -> Option<String> {
    bytes
        .is_empty()
        .then(|| "a message has data or an attribute, and an empty Stream has neither".to_string())
}

/// What the loopback pair agrees on: one project, one topic, one
/// subscription reading it, one bearer token.
const LOOPBACK_PROJECT: &str = "probe";
const LOOPBACK_TOPIC: &str = "probe";
const LOOPBACK_SUBSCRIPTION: &str = "probe-xmip";
const LOOPBACK_TOKEN: &str = "ya29.probe";

#[derive(Clone)]
pub struct PubSubTransport {
    endpoint: String,
    project: String,
    topic: String,
    subscription: String,
    token: String,
    timeout: Option<Duration>,
}

impl PubSubTransport {
    /// Speak to the endpoint at `endpoint` — `https://pubsub.googleapis.com`
    /// in the cloud, `http://host:port` for a stand-in — in `project`,
    /// publishing to `topic` and pulling from `subscription`.
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        project: &str,
        topic: &str,
        subscription: &str,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            project: project.to_string(),
            topic: topic.to_string(),
            subscription: subscription.to_string(),
            token: String::new(),
            timeout: None,
        }
    }

    /// Present this bearer token.
    #[must_use]
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = token.to_string();
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(&self.endpoint, &self.token)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that expects this transport's token and knows its
    /// subscription as a reader of its topic, for a test or the playground
    /// to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session =
            Session::new(&self.token).subscribing(&self.subscription_name(), &self.topic_name());
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// This transport's topic in full: `projects/<p>/topics/<t>`.
    #[must_use]
    pub fn topic_name(&self) -> String {
        format!("projects/{}/topics/{}", self.project, self.topic)
    }

    /// This transport's subscription in full:
    /// `projects/<p>/subscriptions/<s>`.
    #[must_use]
    pub fn subscription_name(&self) -> String {
        format!(
            "projects/{}/subscriptions/{}",
            self.project, self.subscription
        )
    }

    /// The topic a target names in full, a name alone in this transport's
    /// project, or its own where it names none.
    fn resolve(&self, target: &str) -> String {
        if target.is_empty() {
            self.topic_name()
        } else if target.starts_with("projects/") {
            target.to_string()
        } else {
            format!("projects/{}/topics/{target}", self.project)
        }
    }
}

impl Transport for PubSubTransport {
    fn name(&self) -> &'static str {
        "google-pub-sub"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Every message one pull hands back, each acknowledged once it is a
    /// Stream.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client()?;
        let subscription = self.subscription_name();
        let pulled = client.pull(&subscription)?;
        let mut arrived = Vec::with_capacity(pulled.len());
        for message in pulled {
            client.acknowledge(&subscription, &[message.ack_id])?;
            arrived.push(Arrived::new(
                format!("pubsub://{subscription}#{}", message.id),
                message.data,
            ));
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if let Some(why) = refusal(bytes) {
            return Err(TransportError::permanent(why));
        }
        ceiling::within(bytes.len(), ceiling(), "one Pub/Sub message carries")?;
        self.client()?
            .publish(&self.resolve(target), bytes)
            .map(|_| ())
    }
}

impl PubSubTransport {
    /// Both ends on this machine: an ephemeral local port, one token the
    /// far end expects and the near end presents, the loopback timeout.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new(
            "http://127.0.0.1:0",
            LOOPBACK_PROJECT,
            LOOPBACK_TOPIC,
            LOOPBACK_SUBSCRIPTION,
        )
        .with_token(LOOPBACK_TOKEN)
        .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Loopback for PubSubTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn refuses(&self, payload: &[u8]) -> Option<String> {
        refusal(payload)
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let mut session = self.session();
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| match session.serve_one(listener)? {
                Event::Published(arrived) => Ok(arrived),
                Event::Refused(status) => {
                    Err(protocol_error(format!("the session refused: {status}")))
                }
                other => Err(protocol_error(format!("not a publish: {other:?}"))),
            },
            socket::bind_tcp(&endpoint::authority(&self.endpoint)?)?,
        )))
    }

    /// Publish the payload as one message, from a fresh near end
    /// presenting this transport's token, at the endpoint on `address`.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            endpoint: format!("http://{address}"),
            ..self.clone()
        };
        near.send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::JoinHandle;

    fn node(endpoint: &str, token: &str) -> PubSubTransport {
        PubSubTransport::new(endpoint, "partner-x", "orders", "orders-xmip")
            .with_token(token)
            .timing_out_after(Duration::from_secs(2))
    }

    fn serve(
        mut session: Session,
        listener: TcpListener,
        requests: usize,
    ) -> JoinHandle<(Session, Vec<Event>)> {
        std::thread::spawn(move || {
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        })
    }

    #[test]
    fn what_is_published_to_a_session_is_pulled_back_and_acknowledged() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // Two publishes, one pull, then an acknowledge per message.
        let far_end = serve(near.session(), listener, 5);
        near.send("", b"UNA:+.? '").expect("its own topic");
        near.send("orders", &[0, 0xff, b'\r', b'\n'])
            .expect("a name alone");
        let arrived = near.receive().expect("received");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"UNA:+.? '");
        assert_eq!(arrived[1].bytes, [0, 0xff, b'\r', b'\n']);
        assert!(
            arrived[0]
                .origin_uri
                .starts_with("pubsub://projects/partner-x/subscriptions/orders-xmip#")
        );
        let (session, events) = far_end.join().expect("thread");
        assert!(session.messages().is_empty(), "acknowledged after receive");
        assert_eq!(
            events[0],
            Event::Published(Arrived::new(
                "pubsub://projects/partner-x/topics/orders#1",
                b"UNA:+.? '".to_vec()
            ))
        );
        assert!(matches!(&events[2], Event::Pulled { count: 2, .. }));
        assert_eq!(events[3], Event::Acknowledged(vec!["1".to_string()]));
        assert_eq!(
            near.resolve("projects/other/topics/t"),
            "projects/other/topics/t"
        );
    }

    #[test]
    fn a_wrong_token_is_refused_with_the_apis_own_status_and_message() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = serve(node("http://x", "ya29.token").session(), listener, 1);
        let failure = node(&format!("http://{address}"), "ya29.other")
            .send("", b"x")
            .expect_err("refused");
        assert!(
            failure
                .message
                .contains("401 Request had invalid credentials"),
            "{failure}"
        );
        assert!(!failure.retryable);
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(events, vec![Event::Refused("UNAUTHENTICATED".to_string())]);
    }

    #[test]
    fn a_subscription_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1", "t");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "google-pub-sub");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(near.receive().expect_err("nothing listening").retryable);
        assert!(
            !node("pubsub.local", "t")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
    }

    #[test]
    fn what_pub_sub_does_not_carry_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1", "t");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("10485760"), "{failure}");
        let failure = near.send("", b"").expect_err("empty");
        assert!(!failure.retryable);
        assert!(failure.message.contains("neither"), "{failure}");
        assert!(refusal(b"x").is_none());
    }

    #[test]
    fn a_message_rounds_through_the_loopback_session() {
        let loopback = PubSubTransport::loopback();
        let arrived = loopback.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert_eq!(arrived.origin_uri, "pubsub://projects/probe/topics/probe#1");
        assert_eq!(loopback.name(), "google-pub-sub");
        assert_eq!(loopback.ceiling(), Some(ceiling()));
        assert!(loopback.refuses(&[0xff]).is_none());
        assert!(loopback.refuses(b"").is_some());
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'x'; ceiling()]),
        ]
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_the_rest() {
        let loopback = PubSubTransport::loopback();
        let mut carried = 0;
        for (name, payload) in edge_payloads() {
            match loopback.refuses(&payload) {
                None => {
                    let arrived = loopback.round(&payload).expect(name);
                    assert_eq!(arrived.bytes, payload, "{name}");
                    carried += 1;
                }
                Some(why) => {
                    let failure = loopback.round(&payload).expect_err(name);
                    assert!(failure.message.starts_with("send failed:"), "{failure}");
                    assert!(failure.message.contains(&why), "{name}: {failure}");
                }
            }
        }
        assert_eq!(carried, 6, "everything but the empty one");
        let over = vec![b'x'; ceiling() + 1];
        let failure = loopback.round(&over).expect_err("over the brim");
        assert!(failure.message.contains("10485760"), "{failure}");
    }
}
