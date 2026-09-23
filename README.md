# Multithreaded TCP Chat Server

A concurrent, broadcast-style chat server and client built on raw Java sockets. One server accepts many clients at once, and any message a client sends is broadcast to everyone else in real time.

The project is intentionally built on the standard library alone to make the networking and concurrency mechanics visible rather than hidden.

## Features

- **Concurrent clients** — many users are served simultaneously, not one at a time.
- **Real-time broadcast** — a message from any client reaches all others instantly.
- **Direct messages** — `@username message` goes to one person, via an `O(1)` registry lookup.
- **Two concurrency strategies** — a bounded thread pool (capped resources, explicit back-pressure) or virtual threads (one per client, scales to thousands), switchable with a flag.
- **Real back-pressure** — when the pool and its queue are both full, new clients are refused with a reason instead of piling up or stalling the accept loop.
- **No stalled client can hurt another** — per-client outbound queues mean one slow reader never blocks a broadcast, and a client that falls too far behind is dropped deliberately.
- **Bounded waits** — an unfinished handshake times out instead of owning a worker thread forever.
- **Graceful disconnects** — clients leaving cleanly (`BYE`) or abruptly (killed process) are both handled without crashing the server.
- **Thread-safe shared state** — the connected-client registry is safe to read and write from many threads at once.

## How It Works

```
  new connection
        │
        ▼
  ServerSocket (backlog 1024)
        │
        │  accept()          ← the main thread does nothing else, ever
        ▼
  ┌──────────────┐
  │   executor   │   --pool     max threads + a bounded queue; when both are
  │              │              full, refuse with "Server is full"
  │              │   --virtual  one virtual thread per client, no refusals
  └──────┬───────┘
         │  one ClientHandler session per client
         ▼
  ClientHandler ──reads lines──►  "hello"  │  "@bob hi"  │  "BYE"
         │
         │  broadcast / look up a DM target
         ▼
  ConcurrentHashMap<username, ClientHandler>      ← the shared registry
         │
         │  send() = enqueue only; never blocks the sender
         ▼
  the target's bounded outbox
         │
         │  drained by that client's own virtual writer thread
         ▼
  socket ──────────────────────────────────────►  that client
```

The design separates three jobs that a naive server wrongly does on one thread:

- **The main thread only accepts connections.** It loops on `accept()` and hands each new connection off, so the listening socket is never left unattended while a client is being served.
- **Each client session runs on its own thread** (a pooled platform thread, or a virtual thread). The handler reads that client's messages and broadcasts them, independently of every other client.
- **Each client's outgoing messages are written by a dedicated virtual thread** draining that client's queue, so whoever *produces* a message never waits on whoever *receives* it.

**Why a thread pool instead of a thread per client?** A new platform thread per connection is unbounded. Thousands of clients would exhaust memory and cause an OOM. A fixed pool reuses a capped set of workers and queues the overflow, giving predictable resource use and back-pressure. Virtual threads change this calculus entirely, because they aren't OS threads — see [Measured Results](#measured-results) for what each strategy actually costs.

**Why `Runnable` for the handler?** It decouples the *task* (handling a client) from *how it runs*. That's what let the server move from raw threads to a pool, and then to virtual threads, without changing the handler at all.

**Why `ConcurrentHashMap` for the client registry?** Keying by username gives the `O(1)` lookup that direct messaging needs and a list can't. It is thread-safe under concurrent joins, leaves, and lookups with fine-grained internal locking, so clients never block each other on a global lock. Its iterators are weakly consistent, so broadcasting never throws even while someone joins or leaves mid-broadcast.

**Why `AbortPolicy` and not `CallerRunsPolicy`?** `CallerRunsPolicy` runs the rejected task *on the calling thread* — here, the accept loop. The main thread would then spend an entire client session inside one handler and stop calling `accept()`. Connections would queue in the kernel instead, and once that queue overflowed they would hang (see below). Aborting turns overload into an immediate, explicit refusal and keeps the accept loop free.

**Why a large listen backlog?** The backlog is a queue *below* the application: connections the kernel has finished handshaking but the server hasn't `accept()`ed yet. When it overflows, Linux drops the connection **without sending an RST** — but the client's handshake has already completed, so `connect()` succeeds and the client then waits on a socket that no one on the other side owns. The result is a hang with no error, which is the worst possible failure mode. Keeping the backlog large means refusals come from the executor, where the server can answer with a reason.

**Why per-client outbound queues?** A broadcaster writing straight into every socket blocks as soon as one client stops reading and its send buffer fills. That one client would stall everyone else's messages (head-of-line blocking) and, on the bounded pool, hold one of the few worker slots hostage. Queuing the message instead makes every broadcast non-blocking. The queue is bounded, so a client that falls `--outbox` messages behind is dropped on purpose — the same policy IRC servers call an exceeded SendQ.

**Why a separate reader thread in the client?** Because messages arrive at any time, not just as replies. One thread listens for incoming broadcasts and prints them, the main thread reads the keyboard and sends. They run concurrently, so receiving never blocks sending.

## Concepts & Packages

| Area                   | Used for                            | Key APIs                                                         |
| ---------------------- | ----------------------------------- | ---------------------------------------------------------------- |
| `java.net`             | The network layer                   | `ServerSocket`, `Socket`, `accept()`, `setSoTimeout`              |
| `java.io`              | Text over the socket                | `BufferedReader`, `PrintWriter`, `InputStreamReader`              |
| `java.util.concurrent` | Bounded concurrency & safe sharing  | `ThreadPoolExecutor`, `ArrayBlockingQueue`, `ConcurrentHashMap`   |
| `java.lang.Thread`     | Virtual threads & the client reader | `Thread.ofVirtual()`, `Runnable`                                  |

Core ideas demonstrated: blocking I/O, the accept/handle split, thread pools versus virtual threads, back-pressure, head-of-line blocking, the kernel listen backlog, and thread-safe collections.

## Project Layout

```
.
├── Server.java     # Accept loop, executor strategies, broadcast & direct messages
├── Client.java     # Connects, sends keyboard input, prints incoming messages
├── LoadTest.java   # Fires N simultaneous connections and reports how each ended
├── LICENSE
└── README.md
```

- `Server` holds the accept loop and an inner `ClientHandler` (`Runnable`) that runs one client's session.
- `Client` runs a main send-loop plus a daemon reader thread for incoming messages.
- `LoadTest` is the measurement tool: it does the real handshake, holds each connection, and classifies every outcome.

## Requirements

- **JDK 21 or newer — required, not optional.** Verify with `java -version`. Virtual threads (`Thread.ofVirtual()`, `Executors.newVirtualThreadPerTaskExecutor()`) are Java 21 APIs, and all three files use them: `LoadTest` for its generator threads, and `Server` for the per-client writer threads in *both* executor modes. Compiling with `--release 17` fails.
- A terminal. No external dependencies or build tools.
- Linux or macOS for the commands below; the code itself is platform-independent.

## Running the Application

Compile:

```bash
javac Server.java Client.java LoadTest.java
```

Start the server:

```bash
java Server                  # bounded thread pool (the default)
java Server --virtual        # one virtual thread per client
```

In separate terminals, start one or more clients:

```bash
java Client
```

Type in any client and the message appears in all the others. Use `@user message` for a direct message and `BYE` to leave.

An actual session, with two clients. The first thing the server asks for is a username; everything after that is chat. Lines you type are marked `<` here (your terminal echoes them; the server does not):

```
$ java Client
Enter a username:
alpha                                    <
Welcome, alpha! Use '@user message' for a direct message, or 'BYE' to leave.
[Server] beta joined the chat.
beta: hi all
[DM from beta] secret
[Server] beta left the chat.
BYE                                      <
Disconnected from chat.
```

Meanwhile, in the second terminal:

```
$ java Client
Enter a username:
beta                                     <
Welcome, beta! Use '@user message' for a direct message, or 'BYE' to leave.
hi all                                   <
@alpha secret                            <
[DM to alpha] secret
BYE                                      <
Disconnected from chat.
```

Note that `beta` never sees its own `hi all` echoed back: broadcasts go to everyone *except* the sender.

## Load Testing

`LoadTest` opens every connection at once (all generator threads wait on one latch) and reports what happened to each:

```bash
java LoadTest                                  # 300 clients, 30ms hold
java LoadTest --clients=5000 --hold=100        # a much bigger burst
```

```
--- Results ---
Connected (served):        30
Refused (server full):     270
Rejected (name taken):     0
Timed out (no response):   0
Failed (error):            0
Total accounted for:       300 / 300
Wall time:                 170 ms
Handshake latency (ms):    p50=77.5  p95=129.3  max=137.8
```

Reading the results:

- **Connected** counts a *completed* handshake — the client was welcomed, not merely accepted at the TCP level.
- **Refused** is the server's own back-pressure answering politely. Against the default pool it should be roughly `clients - (--max + --queue)`.
- **Timed out** means nothing answered before the deadline. That is the signature of a connection lost below the application, such as a listen-backlog overflow.
- **Total accounted for** must equal the client count. If it doesn't, an outcome is being classified twice or not at all.

Two details make the numbers trustworthy:

- **Every socket operation has a deadline.** Without one, a single stranded connection makes the whole run hang, because the run can only finish when its slowest thread does.
- **Usernames are unique per run.** Reusing `loaduser0..N` against a server that still holds the previous run's registrations makes every connection bounce off the duplicate-name check instantly, so the run "passes" in milliseconds without testing anything.

Two extra lines appear when relevant: a breakdown under **Failed** naming each distinct error, and a note beside **Connected** counting clients the server hung up on before their hold elapsed.

## Measured Results

Measured on a 16-core Linux box, OpenJDK 21, loopback. The point of the two executor strategies is that they trade different things; these are the numbers behind that claim.

**1,000 clients, the same code path, one flag apart:**

| | `--virtual` | `--pool --max=1000 --queue=1` |
| --- | --- | --- |
| OS threads in the process | **58** | **1,049** |
| Carrier / worker threads | 16 (`ForkJoinPool-1`) | 999 platform workers + 16 carriers |
| Virtual threads | 2,000 | 1,000 (the writers) |
| Thread stacks, committed | **3.7 MB** | **93 MB** |
| Thread stacks, reserved | 59 MB | 1.05 GB |
| Clients served | 1,000 | 1,000 |

The carrier count is `availableProcessors` — 16 here — and it does **not** grow with clients: 5,000 clients ran on the same 16 carriers with 52 OS threads in the process. Each client costs two virtual threads (its session and its writer) and zero OS threads; a virtual thread blocked in `readLine()` is parked on the heap while one `Read-Poller` thread watches every socket.

Even `--pool` shows 16 carriers, because the per-client writers are virtual threads in both modes.

**Back-pressure, default pool (`--max=10 --queue=20`), 300-client burst:** 30 served, 270 refused with a reason, 0 failed, 300/300 accounted for, in ~180 ms — repeatable across runs. Served count is exactly `max + queue`; with `--max=2 --queue=3` it is exactly 5.

**Virtual threads, 5,000-client burst:** all 5,000 served, 0 failures, ~1.1 s wall time, handshake p50 134 ms / p95 585 ms.

Reproducing the thread measurements while a run is in flight:

```bash
java Server --virtual &                                  # note the pid
java LoadTest --clients=1000 --hold=20000 --read-timeout=21000 &
sleep 6                                                  # sample mid-flight

grep '^Threads:' /proc/<pid>/status                      # OS threads in the process
cat /proc/<pid>/task/*/comm | sort | uniq -c | sort -rn  # what each OS thread is
jcmd <pid> Thread.dump_to_file -format=json /tmp/vt.json # virtual threads, by container
```

For the stack-memory numbers, start the server with `-XX:NativeMemoryTracking=summary` and read the `Thread` line from `jcmd <pid> VM.native_memory summary`. Note that RSS is a poor substitute here: it's dominated by heap growth, not thread stacks.

## Configuration

Both the server and the load generator take flags; the defaults are the constants at the top of each file.

| Flag                 | File            | Default     | Meaning                                              |
| -------------------- | --------------- | ----------- | ---------------------------------------------------- |
| `--pool`/`--virtual` | `Server.java`   | `--pool`    | Executor strategy (they are alternatives)            |
| `--port=N`           | all three       | `5000`      | Port to listen on / connect to                       |
| `--max=N`            | `Server.java`   | `10`        | Worker ceiling for the bounded pool                  |
| `--queue=N`          | `Server.java`   | `20`        | Connections that may wait before refusals start      |
| `--outbox=N`         | `Server.java`   | `1024`      | Messages a client may fall behind before it's dropped |
| `--host=H`           | `Client`/`LoadTest` | `localhost` | Server address                                   |
| `--clients=N`        | `LoadTest.java` | `300`       | Simultaneous connections to attempt                  |
| `--hold=MS`          | `LoadTest.java` | `30`        | How long each client stays connected                 |
| `--connect-timeout=MS` | `LoadTest.java` | `5000`    | Deadline for the TCP connect                         |
| `--read-timeout=MS`  | `LoadTest.java` | `5000`      | Deadline for each read during the handshake          |

Internal constants in `Server.java`, editable but not exposed as flags: the core pool size (4 threads kept alive at idle), the listen backlog (1024), and the handshake timeout (10s).

To connect across machines, pass the server's address as `--host=` and ensure the port is reachable.

## Protocol

Plain line-based text, UTF-8: each newline-terminated line is one message. There is no framing, length prefix, or escaping.

**Handshake.** The server speaks first, with one of two lines:

| Direction       | Line                                             | Meaning                            |
| --------------- | ------------------------------------------------ | ---------------------------------- |
| server → client | `Enter a username:`                              | Accepted; send a username next     |
| server → client | `[Server] Server is full. Please try again later.` | Refused by back-pressure, then closed |
| client → server | `<username>`                                     | Handshake reply (trimmed)          |
| server → client | `Welcome, <username>! Use '@user message' …`     | Registered; the session begins     |
| server → client | `[Server] Username unavailable. Disconnecting.`  | Name taken or empty, then closed   |

**Session.** After the welcome, either side may send at any time:

| Direction       | Line                                  | Meaning                            |
| --------------- | ------------------------------------- | ---------------------------------- |
| client → server | `hello everyone`                      | Broadcast to all others            |
| client → server | `@bob message`                        | Direct message to one user         |
| client → server | `BYE`                                 | Ends the session cleanly           |
| server → client | `alice: hello everyone`               | A broadcast from `alice`           |
| server → client | `[DM from alice] message`             | A direct message you received      |
| server → client | `[DM to bob] message`                 | Echo confirming your DM was sent   |
| server → client | `[Server] alice joined the chat.`     | Someone joined                     |
| server → client | `[Server] alice left the chat.`       | Someone left, cleanly or not       |
| server → client | `[Server] User 'x' not found.`        | DM target isn't connected          |
| server → client | `[Server] Usage: @username message`   | A `@` line with no message body    |

Three rules if you write your own client:

- **A broadcast never comes back to its sender.** Echo your own messages locally if you want to see them.
- **Don't assume the welcome is literally the next line after your username.** You are registered just before the welcome is queued, so another client's join notice can arrive first. Read lines until you see `Welcome,` or `Username unavailable` — that is exactly what `LoadTest` does.
- **Read continuously, in a separate thread.** If you stop reading, the server's messages for you pile up in your bounded outbox; past `--outbox` messages behind, the server disconnects you on purpose. `Client` uses a daemon reader thread for this reason.

## Troubleshooting

| Symptom | Cause and fix |
| ------- | ------------- |
| `Server error: Address already in use` | A server is already on that port. Find it with `ss -ltnp \| grep 5000` and stop it, or start this one with `--port=5001`. |
| `Could not connect to the server. Is the server running on port: 5000 ?` | No listener on that port, or the client's `--port` doesn't match the server's. |
| Server prints `Accept failed: Too many open files` | The process hit its file-descriptor limit — each client costs one. Check `ulimit -n`; a stock 1024 caps you near 1000 clients. Raise it in the shell you launch from: `ulimit -n 65535`. The server survives this and keeps accepting; it does not die. |
| `LoadTest` reports many `Failed (error)` with `Too many open files` | Same limit, hit on the *generator* side. Raise `ulimit -n` or lower `--clients`. |
| Server prints `Dropping slow client X: outbox full` | Working as designed: that client fell `--outbox` messages behind and was disconnected so it couldn't stall everyone else. Expect it during huge join/leave storms; raise `--outbox` if you want more tolerance. |
| `LoadTest` shows `Timed out (no response)` | Something accepted the TCP connection but never served it — classically a listen-backlog overflow, or a server that stopped calling `accept()`. |
| Server seems frozen and no client gets served | Check the accept loop isn't running a client session: a `jcmd <pid> Thread.print` showing `main` inside `ClientHandler.run` rather than `accept` means a rejection policy is executing tasks on the caller. |

To stop the server, press `Ctrl-C` in its terminal. There is no remote shutdown command.

## Possible Extensions

- **Structured message format:** move from plain lines to a small typed protocol (e.g. JSON with a `type` field) to support commands, timestamps, and metadata.
- **Rooms:** broadcasting every join and leave to everyone is inherently `O(N²)` at `N` members — the cost the `--outbox` limit exists to contain. Scoping broadcasts to a room is the real fix.
- **Persistence:** log chat history to a file or database so messages survive a restart.
- **Non-blocking I/O:** `SocketChannel` with a selector would remove the per-client writer thread entirely, at the cost of much less obvious code.
