import {
  Effect,
  Layer,
  Console,
  Duration,
  Queue,
  Schema,
  Schedule,
  Context,
} from "effect";
import * as Socket from "@effect/platform/Socket";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeSocket from "@effect/platform-node/NodeSocket";

/**
 * Schema definition for Nostr events
 * @see https://github.com/nostr-protocol/nips/blob/master/01.md
 */
const Event = Schema.Struct({
  id: Schema.String,
  pubkey: Schema.String,
  created_at: Schema.Number,
  kind: Schema.Number,
  tags: Schema.Array(Schema.Array(Schema.String)),
  content: Schema.String,
  sig: Schema.String,
});

/**
 * Union schema for all possible server message types
 * Includes EVENT, EOSE, NOTICE, and OK message formats
 */
const ServerMsg = Schema.Union(
  Schema.Tuple(Schema.Literal("EVENT"), Schema.String, Event),
  Schema.Tuple(Schema.Literal("EOSE"), Schema.String),
  Schema.Tuple(Schema.Literal("NOTICE"), Schema.String),
  Schema.Tuple(
    Schema.Literal("OK"),
    Schema.String,
    Schema.Boolean,
    Schema.String
  )
);

/**
 * Schema for Nostr subscription filters
 * All fields are optional and can be used to narrow down event subscriptions
 */
const Filter = Schema.Struct({
  kinds: Schema.optional(Schema.Array(Schema.Number)),
  authors: Schema.optional(Schema.Array(Schema.String)),
  limit: Schema.optional(Schema.Number),
  since: Schema.optional(Schema.Number),
  until: Schema.optional(Schema.Number),
});

/**
 * Parses raw JSON messages from the Nostr relay
 * @param raw - Raw JSON string from the WebSocket connection
 * @returns Effect that parses and validates the message against the ServerMsg schema
 * @throws Error if JSON parsing fails or message doesn't match expected schema
 */
const parseServerMsg = (raw: string) =>
  Effect.try({
    try: () => JSON.parse(raw) as unknown,
    catch: (e) => new Error(`Invalid JSON from relay: ${(e as Error).message}`),
  }).pipe(Effect.flatMap((u) => Schema.decodeUnknown(ServerMsg)(u)));

/**
 * Service interface for Nostr subscription management
 * Provides methods to subscribe to Nostr relays and receive real-time events
 */
export interface NostrSubscription {
  /**
   * Subscribe to a Nostr relay with specified filters
   * @param relayUrl - WebSocket URL of the Nostr relay
   * @param subId - Unique subscription identifier
   * @param filter - Filter criteria for events to receive
   * @returns Effect that manages the subscription lifecycle
   */
  readonly subscribe: (
    relayUrl: string,
    subId: string,
    filter: Schema.Schema.Type<typeof Filter>
  ) => Effect.Effect<void, Error>;
}

/**
 * Context tag for the NostrSubscription service
 * Used for dependency injection throughout the application
 */
export const NostrSubscription =
  Context.GenericTag<NostrSubscription>("NostrSubscription");

/**
 * Creates a new NostrSubscription service instance
 * Manages WebSocket connections, message parsing, and event handling
 * @returns Effect that creates the subscription service
 */
const makeNostrSubscription = Effect.gen(function* () {
  const wsCtor = yield* Socket.WebSocketConstructor;
  const q = yield* Queue.unbounded<string>();

  /**
   * Core subscription logic that handles WebSocket communication
   * @param relayUrl - WebSocket URL to connect to
   * @param subId - Subscription identifier for this connection
   * @param filter - Event filter criteria
   * @returns Effect that manages the subscription
   */
  const subscribeEffect = (
    relayUrl: string,
    subId: string,
    filter: Schema.Schema.Type<typeof Filter>
  ): Effect.Effect<void, Error> =>
    Effect.gen(function* () {
      const ws = yield* Effect.acquireRelease(
        Effect.sync(() => wsCtor(relayUrl)),
        (ws) => Effect.sync(() => ws.close())
      );

      yield* Effect.sync(() => {
        ws.onopen = () => ws.send(JSON.stringify(["REQ", subId, filter]));
        ws.onmessage = (evt: MessageEvent<string>) => q.unsafeOffer(evt.data);
        ws.onerror = (evt) => {
          try {
            q.unsafeOffer(JSON.stringify(["NOTICE", String(evt)]));
          } catch {}
        };
        ws.onclose = () => {
          try {
            q.unsafeOffer(JSON.stringify(["NOTICE", "socket closed"]));
          } catch {}
        };
      });

      yield* Effect.forever(
        Queue.take(q).pipe(
          Effect.flatMap(parseServerMsg),
          Effect.flatMap((msg) => {
            switch (msg[0]) {
              case "EVENT": {
                const [, sid, evt] = msg;
                return Console.log(
                  `EVENT@${sid} kind=${evt.kind} by ${evt.pubkey}:\n${evt.content}\n`
                );
              }
              case "NOTICE": {
                const [, text] = msg;
                return Console.warn(`[NOTICE] ${text}`);
              }
              case "OK": {
                const [, id, ok, text] = msg;
                return Console.log(`[OK] id=${id} accepted=${ok} ${text}`);
              }
              case "EOSE": {
                const [, sid] = msg;
                return Console.log(`EOSE for ${sid} — closing subscription`);
              }
              default:
                return Effect.void;
            }
          })
        )
      );
    }).pipe(
      Effect.retry({
        schedule: Schedule.exponential(Duration.millis(250)).pipe(
          Schedule.intersect(Schedule.duration(Duration.seconds(30)))
        ),
      }),
      Effect.scoped
    );

  return NostrSubscription.of({ subscribe: subscribeEffect });
});

/**
 * Layer that provides the NostrSubscription service
 * Can be composed with other layers for dependency injection
 */
export const NostrSubscriptionLayer = Layer.effect(
  NostrSubscription,
  makeNostrSubscription
);

/**
 * Wraps a subscription with automatic restart logic
 * Handles timeouts and errors by restarting the subscription after delays
 * @param subscription - The subscription service to wrap
 * @param relayUrl - Relay URL for the subscription
 * @param subId - Subscription identifier
 * @param filter - Event filter criteria
 * @returns Effect that runs the subscription with restart logic
 */
const subscribeWithRestart = (
  subscription: NostrSubscription,
  relayUrl: string,
  subId: string,
  filter: Schema.Schema.Type<typeof Filter>
) =>
  Effect.forever(
    subscription.subscribe(relayUrl, subId, filter).pipe(
      Effect.timeout(Duration.seconds(30)),
      Effect.catchTag("TimeoutException", () =>
        Console.log("Subscription timed out — restarting…")
      ),
      Effect.catchAllCause((cause) =>
        Console.warn("Subscription ended with error:", cause)
      ),
      Effect.delay(Duration.seconds(2))
    )
  );

/**
 * Main program that sets up and runs the Nostr client
 * Connects to a relay, subscribes to events, and manages the subscription lifecycle
 * @returns Effect that runs the complete Nostr client application
 */
const program = Effect.gen(function* () {
  const relayUrl = process.env.NOSTR_RELAY ?? "wss://relay.damus.io";
  const subId = "effect-nostr-demo";
  const since = Math.floor(Date.now() / 1000) - 60 * 60;
  const filter = { kinds: [1], limit: 20, since } as const;

  yield* Console.log(
    `Connecting to ${relayUrl} and requesting recent kind:1 notes...`
  );
  const subscription = yield* NostrSubscription;
  yield* subscribeWithRestart(subscription, relayUrl, subId, filter);
});

/**
 * Main application entry point
 * Composes all layers and services, then runs the program
 * Includes error handling for fatal application errors
 */
const main = program.pipe(
  Effect.provide(NostrSubscriptionLayer),
  Effect.provide(NodeSocket.layerWebSocketConstructor),
  Effect.catchAllCause((cause) =>
    Console.error("Fatal error in program:", cause)
  )
);

// Start the application
NodeRuntime.runMain(main);
