//! The far end: enough of Pub/Sub to answer one Location, and what a test
//! or the playground puts on loopback.
//!
//! Not Pub/Sub. One session holds the messages of every topic it is asked
//! about in memory, knows which subscription reads which topic, checks
//! every request for one bearer token, and answers the three calls with
//! the shapes the API answers them — the message ids, the received
//! messages with their ack ids, the error with its status. A pulled
//! message stays in flight until it is acknowledged, as the service keeps
//! it; a pull that finds nothing answers at once.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use transport::Arrived;
use transport::error::Result;

use crate::{ceiling, refusal};
use http::message::{Request, Response};
use http::server;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published a message; here is the Stream, its origin the
    /// topic and the id it was given.
    Published(Arrived),
    /// The client pulled `count` messages from `subscription`.
    Pulled { subscription: String, count: usize },
    /// The client acknowledged these messages, by id.
    Acknowledged(Vec<String>),
    /// The client was answered with this error status.
    Refused(String),
}

/// One message held, on its topic.
#[derive(Clone, Debug)]
struct Held {
    id: String,
    data: Vec<u8>,
    in_flight: bool,
}

pub struct Session {
    token: String,
    topics: BTreeMap<String, Vec<Held>>,
    subscriptions: BTreeMap<String, String>,
    next: usize,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests presenting `token`.
    #[must_use]
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            topics: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Know `subscription` — `projects/<p>/subscriptions/<s>` — as a
    /// reader of `topic` — `projects/<p>/topics/<t>`.
    #[must_use]
    pub fn subscribing(mut self, subscription: &str, topic: &str) -> Self {
        self.subscriptions
            .insert(subscription.to_string(), topic.to_string());
        self
    }

    /// Every message held now, keyed `pubsub://<topic>#<id>`, in flight or
    /// not.
    #[must_use]
    pub fn messages(&self) -> BTreeMap<String, Vec<u8>> {
        self.topics
            .iter()
            .flat_map(|(topic, held)| {
                held.iter()
                    .map(move |m| (origin(topic, &m.id), m.data.clone()))
            })
            .collect()
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        let bearer = format!("Bearer {}", self.token);
        if request.header_value("authorization") != Some(bearer.as_str()) {
            return refused(401, "UNAUTHENTICATED", "Request had invalid credentials");
        }
        let Some((resource, verb)) = request
            .path
            .strip_prefix("/v1/")
            .and_then(|rest| rest.rsplit_once(':'))
        else {
            return refused(404, "NOT_FOUND", "Not a resource and a verb");
        };
        let document: Value = match serde_json::from_slice(&request.body) {
            Ok(document) => document,
            Err(e) => return refused(400, "INVALID_ARGUMENT", &format!("Not JSON: {e}")),
        };
        match (request.method.as_str(), verb) {
            ("POST", "publish") => self.publish(resource, &document),
            ("POST", "pull") => self.pull(resource, &document),
            ("POST", "acknowledge") => self.acknowledge(resource, &document),
            _ => refused(404, "NOT_FOUND", "Not one of the three calls"),
        }
    }

    fn publish(&mut self, topic: &str, document: &Value) -> (Event, Response) {
        let Some(messages) = document["messages"].as_array() else {
            return refused(400, "INVALID_ARGUMENT", "A publish names messages");
        };
        let mut decoded = Vec::with_capacity(messages.len());
        for message in messages {
            let data = match STANDARD.decode(message["data"].as_str().unwrap_or_default()) {
                Ok(data) => data,
                Err(e) => return refused(400, "INVALID_ARGUMENT", &format!("Not base64: {e}")),
            };
            if data.is_empty() && !message["attributes"].is_object() {
                return refused(400, "INVALID_ARGUMENT", &refusal(&data).unwrap_or_default());
            }
            if data.len() > ceiling() {
                let text = format!(
                    "Request payload size exceeds the limit: {} bytes.",
                    ceiling()
                );
                return refused(400, "INVALID_ARGUMENT", &text);
            }
            decoded.push(data);
        }
        let mut ids = Vec::with_capacity(decoded.len());
        let mut first = None;
        for data in decoded {
            let id = self.next.to_string();
            self.next += 1;
            first.get_or_insert_with(|| Arrived::new(origin(topic, &id), data.clone()));
            self.topics
                .entry(topic.to_string())
                .or_default()
                .push(Held {
                    id: id.clone(),
                    data,
                    in_flight: false,
                });
            ids.push(id);
        }
        match first {
            Some(arrived) => (
                Event::Published(arrived),
                answer(&json!({ "messageIds": ids })),
            ),
            None => refused(400, "INVALID_ARGUMENT", "A publish with no messages"),
        }
    }

    fn pull(&mut self, subscription: &str, document: &Value) -> (Event, Response) {
        let Some(topic) = self.subscriptions.get(subscription).cloned() else {
            return refused(
                404,
                "NOT_FOUND",
                &format!("Resource not found: {subscription}"),
            );
        };
        let most = document["maxMessages"].as_u64().unwrap_or(1);
        let mut received = Vec::new();
        for held in self.topics.entry(topic).or_default() {
            if held.in_flight || received.len() as u64 == most {
                continue;
            }
            held.in_flight = true;
            received.push(json!({
                "ackId": format!("ack-{}", held.id),
                "message": { "data": STANDARD.encode(&held.data), "messageId": held.id },
            }));
        }
        let count = received.len();
        let mut body = json!({});
        if count > 0 {
            body["receivedMessages"] = Value::Array(received);
        }
        let event = Event::Pulled {
            subscription: subscription.to_string(),
            count,
        };
        (event, answer(&body))
    }

    fn acknowledge(&mut self, subscription: &str, document: &Value) -> (Event, Response) {
        let Some(topic) = self.subscriptions.get(subscription).cloned() else {
            return refused(
                404,
                "NOT_FOUND",
                &format!("Resource not found: {subscription}"),
            );
        };
        let ids: Vec<String> = document["ackIds"]
            .as_array()
            .map(|each| {
                each.iter()
                    .filter_map(|ack| ack.as_str()?.strip_prefix("ack-"))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let held = self.topics.entry(topic).or_default();
        held.retain(|m| !(m.in_flight && ids.contains(&m.id)));
        (Event::Acknowledged(ids), answer(&json!({})))
    }
}

/// The message `id` on `topic`, as an origin says it.
#[must_use]
pub fn origin(topic: &str, id: &str) -> String {
    format!("pubsub://{topic}#{id}")
}

fn answer(body: &Value) -> Response {
    Response::new(200)
        .header("Content-Type", "application/json; charset=UTF-8")
        .body(body.to_string().as_bytes())
}

fn refused(code: u16, status: &str, message: &str) -> (Event, Response) {
    let body = json!({ "error": { "code": code, "message": message, "status": status } });
    let response = Response::new(code)
        .header("Content-Type", "application/json; charset=UTF-8")
        .body(body.to_string().as_bytes());
    (Event::Refused(status.to_string()), response)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "projects/partner-x/topics/orders";
    const SUBSCRIPTION: &str = "projects/partner-x/subscriptions/orders-xmip";

    fn bearing(path: &str, document: &Value) -> Request {
        Request::new("POST", path)
            .header("Authorization", "Bearer ya29.token")
            .body(document.to_string().as_bytes())
    }

    #[test]
    fn a_session_answers_in_the_apis_shapes_and_refuses_a_wrong_token() {
        let mut session = Session::new("ya29.token").subscribing(SUBSCRIPTION, TOPIC);
        let publish = format!("/v1/{TOPIC}:publish");
        let two = json!({ "messages": [{ "data": "YTxi" }, { "data": "AP8=" }] });
        let (event, response) = session.answer(&bearing(&publish, &two));
        assert_eq!(response.text(), r#"{"messageIds":["1","2"]}"#);
        assert_eq!(
            event,
            Event::Published(Arrived::new(
                "pubsub://projects/partner-x/topics/orders#1",
                b"a<b".to_vec()
            ))
        );
        let pull = format!("/v1/{SUBSCRIPTION}:pull");
        let (event, response) = session.answer(&bearing(&pull, &json!({ "maxMessages": 1 })));
        assert!(response.text().contains(r#""ackId":"ack-1""#));
        assert!(response.text().contains(r#""data":"YTxi""#));
        assert!(matches!(event, Event::Pulled { count: 1, .. }));
        let (event, response) = session.answer(&bearing(&pull, &json!({ "maxMessages": 10 })));
        assert!(
            response.text().contains(r#""ackId":"ack-2""#),
            "the second, not the first again"
        );
        assert!(matches!(event, Event::Pulled { count: 1, .. }));
        let (_, response) = session.answer(&bearing(&pull, &json!({})));
        assert_eq!(response.text(), "{}");
        let acknowledge = format!("/v1/{SUBSCRIPTION}:acknowledge");
        let (event, response) =
            session.answer(&bearing(&acknowledge, &json!({ "ackIds": ["ack-1"] })));
        assert_eq!(
            (event, response.status),
            (Event::Acknowledged(vec!["1".to_string()]), 200)
        );
        assert_eq!(session.messages().len(), 1);
        let empty = json!({ "messages": [{ "data": "" }] });
        let (event, response) = session.answer(&bearing(&publish, &empty));
        assert_eq!(
            (event, response.status),
            (Event::Refused("INVALID_ARGUMENT".to_string()), 400)
        );
        let with_attributes = json!({ "messages": [{ "data": "", "attributes": { "k": "v" } }] });
        let (_, response) = session.answer(&bearing(&publish, &with_attributes));
        assert_eq!(
            response.status, 200,
            "an attribute makes an empty message one"
        );
        let (_, response) = session.answer(&bearing(&publish, &json!({ "messages": [] })));
        assert_eq!(response.status, 400);
        let (_, response) = session.answer(&bearing(
            &publish,
            &json!({ "messages": [{ "data": "!" }] }),
        ));
        assert_eq!(response.status, 400);
        let (_, response) = session.answer(&bearing(
            "/v1/projects/partner-x/subscriptions/x:pull",
            &json!({}),
        ));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&bearing(
            "/v1/projects/partner-x/subscriptions/x:acknowledge",
            &json!({}),
        ));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&bearing("/elsewhere", &json!({})));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&bearing(&format!("/v1/{TOPIC}:delete"), &json!({})));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&bearing(&publish, &json!("not an object")));
        assert_eq!(response.status, 400);
        let broken = Request::new("POST", &publish)
            .header("Authorization", "Bearer ya29.token")
            .body(b"not json");
        let (_, response) = session.answer(&broken);
        assert_eq!(response.status, 400, "not JSON");
        let wrong = Request::new("POST", &publish).header("Authorization", "Bearer other");
        let (event, response) = session.answer(&wrong);
        assert_eq!(
            (event, response.status),
            (Event::Refused("UNAUTHENTICATED".to_string()), 401)
        );
    }
}
