# xmip-core-transport-google-pub-sub

Google Pub/Sub transport: a bearer token over the REST API — publish a Stream as one message to a topic, pull from a subscription and acknowledge each message once the runtime accepts it — a topic and a subscription are a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes to the topic its `project` and `topic` settings name: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which it reads, through `transport::destination::named`, as that topic, its address naming only the endpoint. A target naming another topic — `invoices`, or `projects/<project>/topics/invoices` — still wins (ADR-0064, amendment 2026-11-10).

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

## The Location's secret is its bearer token

A Location names its secret, `secret = "<name>"`: a `password` secret holding the bearer token, kept sealed in the Administration Database and never in configuration, set with `Set-XmipSecret` or `xmip secret --set` (ADR-0019, amendment 2026-10-29). Every publish, pull, acknowledge and nack presents it as `Authorization: Bearer`, read as the request is made (`transport::secured::presented`; ADR-0015, amendment 2026-11-25), so a token set again reaches the next request without a restart. No setting names who presents it: a token names its own subject. A token Google issues expires within the hour, and the operator sets it again before it does; exchanging a service account's key for a token is not built. A Location that names no secret presents an empty token, which Pub/Sub refuses.

## How a pulled message is acknowledged

A receive acknowledges nothing: every message a pull hands on is whole and stays in flight until the runtime gives its verdict after the whole receive cycle (runtime-model section 5). Accepted acknowledges its ack id (`:acknowledge`); Refused acknowledges it too: a subscription's dead-letter policy moves a message only after its maximum delivery attempts, and no call dead-letters one message, so a refused message is acknowledged and not pulled again; the runtime has audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). Failed nacks it (`:modifyAckDeadline` to zero seconds), so the next pull gets it rather than after the subscription's acknowledgement deadline lapses. A crash before the verdict leaves the message to that deadline: at-least-once, never a loss. The acknowledge is the one the receive made until 2026-10-02; the nack is one more request on a kept connection, made only for a failed cycle.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `project`, `topic`, `subscription` and `timeout`, and `SCHEMES`, `google-pub-sub`. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the http technology, and with it TLS — are behind the default feature, and `tls`, which is code, turns the default on with it. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check a Pub/Sub Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
