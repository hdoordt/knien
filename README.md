# Knien

Typed RabbitMQ interfacing for `async` Rust.

This crate defines several types of channels for interfacing with RabbitMQ in various ways:
- `DirectChannel`: a channel for publishing messages on direct queues.
- `TopicChannel`: a channel for publishing messages on topic exchanges
- `RpcChannel`: a channel for publishing messages on direct queues, allowing for receiving replies. It also supports publishing an initial message on a direct queue, and setting up a back-and-forth communincation channel.

*`RpcChannel`s currently require the `tokio` runtime to spawn tasks that receive messages and is therefore behind the `rpc` feature flag.*

Each channel exposes methods to instantiate `Publisher`s and `Consumer`s, each of which are generic over the bus they publish on or consume from. These buses define what types of payload they send, so `Publisher`s and `Consumer`s can take care of serializing and deserializing the payloads and make sure the payloads they publish and yield are of the correct types.

There are several kinds of typed buses, each tied to one of the Channels:
- `DirectBus`: a bus that defines a `PublishPayload` type, a formatter for direct queue names, and the type of argument to that formatter. `DirectBus`es are used to simply publish messages on a direct queue without expecting any response. `DirectBus`es are tied to the `DirectChannel`.
- `TopicBus`: a bus that defines a `PublishPayload` type, a topic, and an `Exchange`. `TopicBus`es are used to publish and consume messages from a topic exchange. `TopicBus`es are tied to the `TopicChannel`.
- `RpcBus`: act like a `DirectBus`, but further defines a `ReplyPayload` type and allows for awaiting replies on sent messages. `RpcBus`es are tied to the `RpcChannel`
- `RpcCommBus`: a bus that defines `InitialPayload`, `BackPayload` and `ForthPayload`, and support flows where a single initial message is sent, after which back-and-forth a communication over which multiple messages can be sent is setup. `RpcCommBus`es are tied to the `RpcChannel`
