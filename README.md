# xmip-core-transport-google-pub-sub

Google Pub/Sub transport: a bearer token over the REST API — publish a Stream as one message to a topic, pull from a subscription and acknowledge each message once the runtime accepts it — a topic and a subscription are a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

## How a pulled message is acknowledged

A receive acknowledges nothing: every message a pull hands on is whole and stays in flight until the runtime gives its verdict after the whole receive cycle (runtime-model section 5). Accepted acknowledges its ack id (`:acknowledge`); Refused acknowledges it too: a subscription's dead-letter policy moves a message only after its maximum delivery attempts, and no call dead-letters one message, so a refused message is acknowledged and not pulled again; the runtime has audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). Failed nacks it (`:modifyAckDeadline` to zero seconds), so the next pull gets it rather than after the subscription's acknowledgement deadline lapses. A crash before the verdict leaves the message to that deadline: at-least-once, never a loss. The acknowledge is the one the receive made until 2026-10-02; the nack is one more request on a kept connection, made only for a failed cycle.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
