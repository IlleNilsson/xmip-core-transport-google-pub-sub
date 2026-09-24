//! Xmip's side: the three calls a Location makes, each one request with a
//! bearer token over one connection.
//!
//! The REST API, because it is one HTTP call per operation and answers in
//! a shape serde reads: `POST /v1/projects/<p>/topics/<t>:publish`,
//! `POST /v1/projects/<p>/subscriptions/<s>:pull`, `POST …:acknowledge`.
//! Obtaining the token is outside this crate: a Location is configured
//! with one — from a service account, a metadata server, or an operator —
//! and hands it on as `Authorization: Bearer`.

use std::time::Duration;

use codec::base64;
use serde_json::{Value, json};
use transport::error::{Result, protocol_error};

use http::endpoint;
use http::message::{self, Request, Response};

/// The most one pull hands back.
pub const MAX_MESSAGES: u8 = 10;

/// One message as it came off the subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Received {
    pub ack_id: String,
    pub id: String,
    pub data: Vec<u8>,
}

pub struct Client {
    endpoint: String,
    host: String,
    token: String,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the Pub/Sub endpoint at `endpoint` — `http://host:port` or
    /// `https://host:port`, `https://pubsub.googleapis.com` in the cloud —
    /// presenting `token`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, token: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.to_string(),
            host: endpoint::authority(endpoint)?,
            token: token.to_string(),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Publish `bytes` as one message to `topic` — `projects/<p>/topics/<t>`
    /// — and learn its id.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached, or answered
    /// with no id.
    pub fn publish(&self, topic: &str, bytes: &[u8]) -> Result<String> {
        let document = json!({ "messages": [{ "data": base64::encode(bytes) }] });
        let answer = self.call(topic, "publish", &document)?;
        answer["messageIds"][0]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| protocol_error("a publish answered with no messageIds"))
    }

    /// Up to [`MAX_MESSAGES`] messages from `subscription` —
    /// `projects/<p>/subscriptions/<s>` — each held until it is
    /// acknowledged.
    ///
    /// # Errors
    /// Where the endpoint refused, could not be reached, or answered with
    /// messages that are not base64.
    pub fn pull(&self, subscription: &str) -> Result<Vec<Received>> {
        let document = json!({ "maxMessages": MAX_MESSAGES });
        let answer = self.call(subscription, "pull", &document)?;
        answer["receivedMessages"].as_array().map_or_else(
            || Ok(Vec::new()),
            |each| each.iter().map(received).collect(),
        )
    }

    /// Acknowledge the messages `ack_ids` were received with: they are
    /// Streams now, and leave the subscription.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached.
    pub fn acknowledge(&self, subscription: &str, ack_ids: &[String]) -> Result<()> {
        let document = json!({ "ackIds": ack_ids });
        self.call(subscription, "acknowledge", &document)
            .map(|_| ())
    }

    fn call(&self, resource: &str, verb: &str, document: &Value) -> Result<Value> {
        let request = Request::new("POST", format!("/v1/{resource}:{verb}"))
            .header("Host", &self.host)
            .header("Authorization", &format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .body(document.to_string().as_bytes());
        let stream = endpoint::connect(&self.endpoint, self.timeout)?;
        let answer = judge(message::exchange(stream, &request)?)?;
        serde_json::from_slice(&answer.body)
            .map_err(|e| protocol_error(format!("an answer that is not JSON: {e}")))
    }
}

/// One received message as the API writes it, read back.
fn received(value: &Value) -> Result<Received> {
    let text = |name: &str| value[name].as_str().unwrap_or_default().to_string();
    let data = base64::decode(value["message"]["data"].as_str().unwrap_or_default())
        .map_err(|e| protocol_error(format!("a message whose data is not base64: {e}")))?;
    Ok(Received {
        ack_id: text("ackId"),
        id: value["message"]["messageId"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        data,
    })
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the message the service put in the body, retryable where HTTP says come
/// back.
///
/// # Errors
/// Where the status is not 2xx.
pub fn judge(response: Response) -> Result<Response> {
    message::judge("Pub/Sub", response, reason, |_| false)
}

/// The message an error answer carries, or nothing.
fn reason(response: &Response) -> String {
    serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|error| error["error"]["message"].as_str().map(str::to_string))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    const TOPIC: &str = "projects/partner-x/topics/orders";
    const SUBSCRIPTION: &str = "projects/partner-x/subscriptions/orders-xmip";

    #[test]
    fn the_three_calls_reach_a_session_and_come_back_shaped_as_the_api_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("ya29.token")
                .subscribing(SUBSCRIPTION, TOPIC)
                .timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..6)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}"), "ya29.token")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        let id = client.publish(TOPIC, b"UNA:+.? '").expect("published");
        assert!(!id.is_empty(), "an id came back");
        client.publish(TOPIC, &[0, 0xff, b'\n']).expect("published");
        let pulled = client.pull(SUBSCRIPTION).expect("pulled");
        assert_eq!(pulled.len(), 2);
        assert_eq!(pulled[0].id, id);
        assert_eq!(pulled[0].data, b"UNA:+.? '");
        assert_eq!(pulled[1].data, [0, 0xff, b'\n']);
        assert!(client.pull(SUBSCRIPTION).expect("pulled").is_empty());
        client
            .acknowledge(SUBSCRIPTION, &[pulled[0].ack_id.clone()])
            .expect("acknowledged");
        let missing = client
            .pull("projects/partner-x/subscriptions/nobody")
            .expect_err("no such subscription");
        assert!(missing.message.contains("404"), "{missing}");
        assert!(!missing.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.messages().len(), 1, "one left in flight");
        assert!(matches!(&events[2], Event::Pulled { count: 2, .. }));
        assert_eq!(events[4], Event::Acknowledged(vec![id]));
        assert_eq!(events[5], Event::Refused("NOT_FOUND".to_string()));
    }

    #[test]
    fn a_server_failure_is_worth_repeating_and_a_client_one_is_not() {
        assert!(judge(Response::new(503)).expect_err("server").retryable);
        assert!(judge(Response::new(429)).expect_err("throttled").retryable);
        let body = br#"{"error":{"code":403,"message":"no","status":"PERMISSION_DENIED"}}"#;
        let failure = judge(Response::new(403).body(body)).expect_err("forbidden");
        assert!(!failure.retryable);
        assert_eq!(failure.message, "Pub/Sub answered 403 no");
        assert!(Client::new("pubsub.local", "t").is_err());
        let nobody = Client::new("http://127.0.0.1:1", "t").expect("ok");
        assert!(nobody.pull(SUBSCRIPTION).expect_err("nobody").retryable);
        assert!(received(&json!({ "message": { "data": "not base64!" } })).is_err());
    }
}
