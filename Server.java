import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * Three upgrades over the base version:
 * 1. ConcurrentHashMap registry keyed by username -> enables direct messages
 * and safe concurrent joins/leaves/lookups.
 * 2. A choice of executor: a bounded ThreadPoolExecutor (back-pressure) OR
 * virtual threads (cheap massive concurrency).
 * 3. Direct messaging via "@username message".
 *
 * Run with: java Server [--pool|--virtual] [--port=N] [--max=N] [--queue=N]
 */
public class Server {

    private static int port = 5000;

    // EXECUTOR STRATEGY: They are alternatives, not combinable.
    // true -> virtual threads: one per client, ~unlimited, ideal for blocking I/O.
    // false -> bounded platform-thread pool: capped concurrency + real
    // back-pressure.
    private static boolean useVirtualThreads = false;

    // Bounded-pool settings:
    private static final int CORE_POOL = 4; // threads kept alive at idle
    private static int maxPool = 10; // ceiling on worker threads
    private static int queueCapacity = 20; // tasks that can wait before we reject

    /**
     * How many completed-but-not-yet-accepted connections the KERNEL may hold.
     * This is a different queue from the executor's: it sits below the
     * application entirely. When it overflows, Linux silently drops the
     * connection — no RST — while the client's handshake has already completed,
     * so the client sits in readLine() forever with no way to learn it was
     * dropped. That is a hang, not back-pressure, so we keep this queue large
     * and let refusals come from the executor, where we can answer politely.
     */
    private static final int BACKLOG = 1024;

    // A client that connects but never sends a username must not own a worker
    // thread forever — in the bounded pool that is one of only maxPool slots.
    private static final int HANDSHAKE_TIMEOUT_MS = 10_000;

    // How long to wait before retrying an accept() that failed.
    private static final int ACCEPT_RETRY_PAUSE_MS = 100;

    // Messages buffered per client before we consider it a hopeless slow reader.
    // A join/leave storm among N clients generates O(N) messages per client, so
    // this needs headroom for the burst, not just the steady state.
    private static int outboxCapacity = 1024;

    /**
     * THE SHARED REGISTRY: username -> that client's session.
     * ConcurrentHashMap:
     * - Keying by username gives O(1) lookup for direct messages (a list can't).
     * - It's thread-safe under concurrent joins/leaves/lookups with fine-grained
     * internal locking — no global lock, so clients don't block each other.
     * - Its iterators are weakly consistent (fail-safe): broadcasting by iterating
     * the map never throws even while another thread adds/removes a client.
     */
    private static final ConcurrentHashMap<String, ClientHandler> clients = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        parseArgs(args);

        ExecutorService pool = createExecutor();
        // Writers never run on the session pool: see ClientHandler.
        ExecutorService writers = Executors.newVirtualThreadPerTaskExecutor();

        System.out.println("Server on port " + port + " using "
                + (useVirtualThreads ? "virtual threads"
                        : "bounded thread pool (max=" + maxPool + ", queue=" + queueCapacity + ")"));

        try (ServerSocket serverSocket = new ServerSocket(port, BACKLOG)) {
            while (true) {
                Socket socket;
                try {
                    socket = serverSocket.accept(); // main thread: accept only
                } catch (IOException e) {
                    // One bad accept (e.g. out of file descriptors) must not take the
                    // whole server down — the listening socket is still good.
                    System.out.println("Accept failed: " + e.getMessage());
                    // The cause is usually resource exhaustion, which persists for a
                    // while and makes accept() fail instantly. Retrying in a tight loop
                    // would spin a core and flood the log, so pause before trying again.
                    try {
                        Thread.sleep(ACCEPT_RETRY_PAUSE_MS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }
                try {
                    // execute, not submit: submit() wraps the task in a FutureTask that
                    // swallows any exception into a Future nobody reads, so handler bugs
                    // would vanish silently.
                    pool.execute(new ClientHandler(socket, writers));
                } catch (RejectedExecutionException e) {
                    // only for ThreadPoolExecutor and not for virtual threads
                    // BACK-PRESSURE in action: pool AND queue are full, so the pool's
                    // AbortPolicy rejected this task. We refuse the client cleanly
                    // instead of letting work pile up until the server dies.
                    rejectConnection(socket);
                }
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        } finally {
            pool.shutdownNow();
            writers.shutdownNow();
        }
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.equals("--virtual")) {
                useVirtualThreads = true;
            } else if (arg.equals("--pool")) {
                useVirtualThreads = false;
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--max=")) {
                maxPool = Integer.parseInt(arg.substring("--max=".length()));
            } else if (arg.startsWith("--queue=")) {
                queueCapacity = Integer.parseInt(arg.substring("--queue=".length()));
            } else if (arg.startsWith("--outbox=")) {
                outboxCapacity = Integer.parseInt(arg.substring("--outbox=".length()));
            } else {
                System.out.println("Unknown option: " + arg);
                System.out.println(
                        "Usage: java Server [--pool|--virtual] [--port=N] [--max=N] [--queue=N] [--outbox=N]");
                System.exit(2);
            }
        }
        // ThreadPoolExecutor and ArrayBlockingQueue both reject these at construction
        // with an opaque IllegalArgumentException, so say what's wrong instead.
        requireAtLeastOne("--max", maxPool);
        requireAtLeastOne("--queue", queueCapacity);
        requireAtLeastOne("--outbox", outboxCapacity);
    }

    private static void requireAtLeastOne(String flag, int value) {
        if (value < 1) {
            System.out.println(flag + " must be at least 1 (got " + value + ").");
            System.exit(2);
        }
    }

    // Builds the chosen executor. This is the one place the two strategies diverge.
    private static ExecutorService createExecutor() {
        if (useVirtualThreads) {
            // One virtual thread per task. Virtual threads are lightweight and don't
            // map 1:1 to OS threads, so hundreds of thousands of clients blocked in
            // readLine() cost almost nothing. No bounding needed or wanted.
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        // Bounded pool: up to maxPool workers, with a BOUNDED queue in front.
        // When both are full, AbortPolicy throws RejectedExecutionException — the
        // explicit back-pressure that Executors.newFixedThreadPool (unbounded queue)
        // silently lacks.
        return new ThreadPoolExecutor(
                // A core larger than the max is an error, so --max=2 must lower the core
                // rather than blow up at construction.
                Math.min(CORE_POOL, maxPool), maxPool,
                60L, TimeUnit.SECONDS, // idle workers above core die after 60s
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.AbortPolicy());
        // AbortPolicy — NOT CallerRunsPolicy — is correct for a server.
        // CallerRuns would execute the handler on the MAIN thread, blocking the accept
        // loop for the whole length of that client's session. New connections would
        // then pile up in the kernel backlog until it overflowed and they hung. We
        // want to reject, not stall accepting.
    }

    // Tell a refused client the server is full, then close.
    private static void rejectConnection(Socket socket) {
        try (Socket s = socket) {
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("[Server] Server is full. Please try again later.");
            // Half-close so the FIN follows the message through the send buffer.
            // Closing outright while data is still in flight can send an RST instead
            // and the client would see a reset rather than the reason it was refused.
            s.shutdownOutput();
        } catch (IOException ignored) {
        }
    }

    // Handles one client's whole session. Runnable, so it runs on either executor
    private static class ClientHandler implements Runnable {

        private final Socket socket;
        private final ExecutorService writers;

        /**
         * Outbound messages wait here instead of being written by whoever produced
         * them. A broadcaster writing directly into 300 sockets blocks the moment
         * one client stops reading and its send buffer fills — that client would
         * then stall every other client's messages (head-of-line blocking) and, on
         * the bounded pool, hold a worker slot hostage. Handing the message to a
         * queue keeps every broadcast non-blocking and bounded.
         */
        private final BlockingQueue<String> outbox = new ArrayBlockingQueue<>(outboxCapacity);
        private static final String POISON = new String("<<close>>"); // identity-compared sentinel

        private String username; // null until the handshake succeeds
        private volatile boolean closed;

        ClientHandler(Socket socket, ExecutorService writers) {
            this.socket = socket;
            this.writers = writers;
        }

        @Override
        public void run() {
            Future<?> writerTask = null;
            // Deliberately NOT try-with-resources: that closes the socket before the
            // finally block runs, which would cut off the writer thread mid-flush and
            // lose the last message — exactly the ones that explain the disconnect,
            // like "Username unavailable". The finally below flushes, then closes.
            Socket clientSocket = this.socket;
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), false);

                // One virtual thread drains this client's outbox. It is deliberately NOT
                // taken from the session pool: with maxPool=10, writers competing for the
                // same 10 slots would deadlock (handlers waiting on writers that can never
                // start). Virtual threads are cheap enough to spend one per connection.
                writerTask = writers.submit(() -> drainOutbox(writer));

                // USERNAME HANDSHAKE
                send("Enter a username:");
                clientSocket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
                String name = in.readLine();
                if (name == null)
                    return; // left during handshake
                name = name.trim();

                // putIfAbsent is ATOMIC: it registers the name only if free, and
                // reports failure otherwise in one operation.
                if (name.isEmpty() || clients.putIfAbsent(name, this) != null) {
                    send("[Server] Username unavailable. Disconnecting.");
                    return;
                }
                this.username = name;

                // Our own welcome goes in the outbox FIRST. If it were queued after the
                // join broadcast, a mass join would fill this client's outbox with other
                // people's notices and its own confirmation could be starved out behind
                // them — the client would sit waiting for a welcome that never arrives.
                send("Welcome, " + username
                        + "! Use '@user message' for a direct message, or 'BYE' to leave.");
                broadcast("[Server] " + username + " joined the chat.", username);

                // SESSION LOOP. No read timeout here: an idle chat client is normal,
                // unlike one that never finishes the handshake.
                clientSocket.setSoTimeout(0);
                String line;
                while ((line = in.readLine()) != null) { // null = client left
                    if (line.equalsIgnoreCase("BYE"))
                        break;
                    if (line.startsWith("@")) {
                        sendDirect(line);
                    } else {
                        broadcast(username + ": " + line, username);
                    }
                }

            } catch (SocketTimeoutException e) {
                // Handshake deadline expired — reclaim the worker.
            } catch (SocketException e) {
                System.out.println("Client " + (username == null ? "<unnamed>" : username)
                        + " disconnected abruptly.");
            } catch (IOException e) {
                System.out.println("Connection error: " + e.getMessage());
            } finally {
                // No further messages for us, but whatever is already queued still gets
                // written.
                closed = true;
                // Always deregister so we never broadcast to a dead connection.
                // Two-arg remove: only ever drop OUR entry, never a later session
                // that reused the same username.
                if (username != null) {
                    clients.remove(username, this);
                    broadcast("[Server] " + username + " left the chat.", username);
                }
                stopWriter(writerTask); // flushes the outbox
                closeQuietly(clientSocket); // then, and only then, send the FIN
            }
        }

        private void closeQuietly(Socket socket) {
            try {
                socket.close(); // also closes the streams layered over it
            } catch (IOException ignored) {
            }
        }

        /**
         * The only place this client's socket is written. Drains everything queued
         * before flushing, so a 300-client join storm costs one flush per burst
         * rather than one per message.
         */
        private void drainOutbox(PrintWriter writer) {
            try {
                while (true) {
                    String message = outbox.take();
                    if (message == POISON)
                        return;
                    do {
                        writer.println(message);
                    } while ((message = outbox.poll()) != null && message != POISON);
                    writer.flush();
                    if (writer.checkError() || message == POISON)
                        return; // socket is gone (PrintWriter swallows the IOException)
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Queue a message for this client. Never blocks the calling thread. */
        private void send(String message) {
            if (closed)
                return;
            if (!outbox.offer(message)) {
                // outboxCapacity messages behind and still not reading: this client
                // cannot keep up. Drop it rather than let it consume memory or slow
                // everyone else down. Closing the socket makes its handler's
                // readLine() return, which runs the normal cleanup path.
                // (This is the same policy IRC servers call an exceeded SendQ.)
                closed = true;
                System.out.println("Dropping slow client "
                        + (username == null ? "<unnamed>" : username)
                        + ": outbox full (" + outboxCapacity + " messages behind).");
                closeQuietly(socket);
            }
        }

        private void stopWriter(Future<?> writerTask) {
            if (writerTask == null)
                return;
            outbox.offer(POISON); // may fail if the outbox is full; the cancel below covers it
            try {
                writerTask.get(1, TimeUnit.SECONDS); // let queued messages flush
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                // fall through to cancel
            } finally {
                writerTask.cancel(true);
            }
        }

        // Send to everyone except the sender. Iterating the map is fail-safe.
        private void broadcast(String message, String sender) {
            for (var entry : clients.entrySet()) {
                if (!entry.getKey().equals(sender)) {
                    entry.getValue().send(message);
                }
            }
        }

        // Handle "@username message" O(1) lookup by key
        private void sendDirect(String line) {
            int space = line.indexOf(' ');
            if (space == -1) {
                send("[Server] Usage: @username message");
                return;
            }

            String target = line.substring(1, space);
            String message = line.substring(space + 1);
            ClientHandler targetClient = clients.get(target); // O(1) key lookup

            if (targetClient == null) {
                send("[Server] User '" + target + "' not found.");
            } else {
                targetClient.send("[DM from " + username + "] " + message);
                send("[DM to " + target + "] " + message);
            }
        }
    }
}
