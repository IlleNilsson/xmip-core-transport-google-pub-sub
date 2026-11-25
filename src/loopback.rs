//! The transport as its own far end (ADR-0051): the session stands up at
//! the endpoint's authority and takes the one publish.

use std::net::TcpListener;

use net::Endpoint;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::{ArrivalIdentity, Configured, Transport, socket};

use crate::{Event, PubSubTransport, ceiling, refusal};

/// What the loopback pair agrees on: one project, one topic, one
/// subscription reading it, one bearer token.
const LOOPBACK_PROJECT: &str = "probe";
const LOOPBACK_TOPIC: &str = "probe";
const LOOPBACK_SUBSCRIPTION: &str = "probe-xmip";
const LOOPBACK_TOKEN: &str = "ya29.probe";

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
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed("the subscription delivers it: its attributes say who sent it")
    }

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
            socket::bind_tcp(&Endpoint::parse(&self.endpoint)?.address())?,
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
