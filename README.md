# Effect Nostr Client

A modern, type-safe Nostr client built with [Effect](https://effect.website/) v3 for real-time event streaming.

## 🚀 Quick Start

```bash
# Install & start
bun install
bun start

# Custom relay
export NOSTR_RELAY="wss://your-relay.com"
bun start
```

## ✨ Features

- Real-time Nostr events with automatic reconnection
- Effect v3 architecture with full TypeScript support
- Event filtering and robust error handling
- WebSocket management with proper cleanup

## 🏗️ Architecture

- **`NostrSubscription`**: Service interface for subscriptions
- **`NostrSubscriptionLayer`**: Effect layer for dependency injection
- **Automatic restart** with exponential backoff
- **Message parsing** for EVENT, EOSE, NOTICE, and OK messages

## 📡 Current Configuration

- **Event Kind**: 1 (text notes)
- **Limit**: 20 events
- **Time Range**: Last hour
- **Default Relay**: `wss://relay.damus.io`

## 📚 Resources

- [Effect Documentation](https://effect.website/)
- [Nostr Protocol](https://github.com/nostr-protocol/nips)
- [Bun Runtime](https://bun.sh/)

---

Built with ❤️ using Effect v3 and TypeScript
