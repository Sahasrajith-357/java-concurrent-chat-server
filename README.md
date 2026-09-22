# Multithreaded TCP Chat Server

A concurrent, broadcast-style chat server and client built on raw Java sockets. One server accepts many clients at once, Any message a client sends is broadcast to everyone else in real time.

The project is intentionally built on the standard library alone to make the networking and concurrency mechanics visible rather than hidden.

## Features

- **Concurrent clients** — many users are served simultaneously, not one at a time.
- **Real-time broadcast** — a message from any client reaches all others instantly.
- **Bounded concurrency** — a fixed thread pool caps resource use, so a flood of connections can't spawn unlimited threads and cause resource exhaustion.
- **Graceful disconnects** — clients leaving cleanly (`BYE`) or abruptly (killed process) are both handled without crashing the server.
- **Thread-safe shared state** — the connected-client registry is safe to read and write from many threads at once.

## How It Works

The design separates two jobs that a naive server wrongly does on one thread:

- **The main thread only accepts connections.** It loops on `accept()` and hands each new connection off, so the listening socket is never left unattended while a client is being served.
- **Each client session runs on a pooled worker thread.** The handler reads that client's messages and broadcasts them, independently of every other client.

**Why a thread pool instead of a thread per client?** A new thread per connection is unbounded. Thousands of clients would exhaust memory and cause OOM exception. A fixed pool reuses a capped set of workers and queues the overflow, giving predictable resource use and back-pressure.

**Why `Runnable` for the handler?** It decouples the *task* (handling a client) from *how it runs*. That's what let the server move from raw threads to a pool without changing the handler at all.

**Why `CopyOnWriteArrayList` for the client registry?** Every worker iterates the shared list to broadcast, but the list changes only when someone joins or leaves which is a read-heavy, write-rare workload. This collection iterates over a stable snapshot, so broadcasting never breaks even while a client joins or leaves mid-broadcast. A plain `ArrayList` would corrupt or throw under that concurrency.

**Why a separate reader thread in the client?** Because messages arrive at any time, not just as replies. One thread listens for incoming broadcasts and prints them, the main thread reads the keyboard and sends. They run concurrently, so receiving never blocks sending.

## Concepts & Packages

| Area                   | Used for                           | Key APIs                                               |
| ---------------------- | ---------------------------------- | ------------------------------------------------------ |
| `java.net`             | The network layer                  | `ServerSocket`, `Socket`, `accept()`                   |
| `java.io`              | Text over the socket               | `BufferedReader`, `PrintWriter`, `InputStreamReader`   |
| `java.util.concurrent` | Bounded concurrency & safe sharing | `ExecutorService`, `Executors`, `CopyOnWriteArrayList` |
| `java.lang.Thread`     | The client's independent reader    | `Runnable`, `Thread`                                   |

Core ideas demonstrated: blocking I/O, the accept/handle split, thread pools, shared mutable state, and thread-safe collections.

## Project Layout

```
.
├── Server.java   # Accepts connections, pools workers, broadcasts messages
├── Client.java   # Connects, sends keyboard input, prints incoming messages
└── README.md
```

- `Server` holds the accept loop and an inner `ClientHandler` (`Runnable`) that runs one client's session.
- `Client` runs a main send-loop plus a daemon reader thread for incoming messages.

## Requirements

- **JDK 21** (or any JDK 17+). Verify with `java -version` and `javac -version`.
- A terminal. No external dependencies or build tools.

## Running the Application

Compile both files:

```bash
javac Server.java Client.java
```

Start the server:

```bash
java Server
```

In separate terminals, start one or more clients:

```bash
java Client
```

Type in any client and the message appears in all the others. Type `BYE` to leave.

## Configuration

Settings are constants at the top of each file:

| Setting         | File          | Default              | Meaning                               |
| --------------- | ------------- | -------------------- | ------------------------------------- |
| `PORT`          | `Server.java` | `5000`               | Port the server listens on            |
| `POOL_SIZE`     | `Server.java` | `10`                 | Max clients served simultaneously     |
| `HOST` / `PORT` | `Client.java` | `localhost` / `5000` | Server address the client connects to |

To connect across machines, set the client's `HOST` to the server's IP and ensure the port is reachable. Keep the client's `PORT` equal to the server's.

## Protocol

Plain line-based text: each newline-terminated line is one message. The single reserved command is `BYE`, which ends a client's session cleanly.

## Possible Extensions

Each of these addresses a real limitation of the current design.

### Usernames & direct messages — `ConcurrentHashMap`

**Limitation:** clients are anonymous, and the registry is a flat list => you can broadcast, but you can't message one specific person.

**Approach:** replace the `CopyOnWriteArrayList` with a `ConcurrentHashMap<String, PrintWriter>` keyed by username. Broadcasting iterates the values; a direct message looks up one key.

**Why `ConcurrentHashMap`:** it's a thread-safe map with fine-grained internal locking, so concurrent joins, leaves, and lookups don't block each other or corrupt the map. A key-based lookup (`O(1)` by username) is exactly what direct messaging needs, which a list can't offer.

### Back-pressure under load — bounded `ThreadPoolExecutor`

**Limitation:** `Executors.newFixedThreadPool` caps the *threads* but backs them with an *unbounded* task queue. Under a connection flood, threads stay capped (good) but the queue grows without limit and can exhaust memory (bad).

**Approach:** construct a `ThreadPoolExecutor` directly with a **bounded** queue (e.g. `ArrayBlockingQueue`) and a **`RejectedExecutionHandler`**. When both the pool and the queue are full, new connections are refused cleanly instead of piling up.

**Why:** this turns silent overload into explicit back-pressure. The server sheds load deliberately rather than degrading until it crashes. That's the difference between the convenient preset and a production-grade pool.

### Other directions

- **Virtual threads (Java 21+):** swap the pool for `Executors.newVirtualThreadPerTaskExecutor()` to scale one-thread-per-client to hundreds of thousands of connections cheaply, since virtual threads are lightweight and don't map 1:1 to OS threads.
- **Structured message format:** move from plain lines to a small typed protocol (e.g. JSON with a `type` field) to support commands, timestamps, and metadata.
- **Persistence:** log chat history to a file or database so messages survive a restart.
