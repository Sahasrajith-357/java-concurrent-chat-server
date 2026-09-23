import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Load generator for the chat server. Opens CLIENTS connections at once, each
 * on its own virtual thread, does the username handshake, holds the connection
 * for a moment, then closes. Reports how each attempt ended.
 *
 * Every socket operation has a deadline. Without one a single stuck connection
 * hangs the whole run: when a server's listen backlog overflows, Linux drops
 * the connection without ever sending an RST, yet the client's handshake has
 * already completed — so connect() succeeds and the following read blocks
 * forever with nothing on the other end.
 *
 * Run with:
 * java LoadTest [--clients=N] [--hold=MS] [--host=H] [--port=N]
 * [--connect-timeout=MS] [--read-timeout=MS]
 */
public class LoadTest {

    private static String host = "localhost";
    private static int port = 5000;
    private static int clients = 300; // how many simultaneous connections to attempt
    private static int holdMillis = 30; // keep each connection open this long
    private static int connectTimeoutMillis = 5_000;
    private static int readTimeoutMillis = 5_000;

    // Thread-safe counters: many generator threads increment these at once.
    private static final AtomicInteger connected = new AtomicInteger();
    private static final AtomicInteger serverFull = new AtomicInteger();
    private static final AtomicInteger nameTaken = new AtomicInteger();
    private static final AtomicInteger timedOut = new AtomicInteger();
    private static final AtomicInteger failed = new AtomicInteger();
    // A subset of `connected`: served, then hung up on before the hold elapsed.
    private static final AtomicInteger droppedDuringHold = new AtomicInteger();

    // What the failures actually were, so "Failed: 354" is diagnosable instead of
    // being a number you have to go and reproduce under a debugger.
    private static final ConcurrentHashMap<String, AtomicInteger> failureKinds = new ConcurrentHashMap<>();

    // Handshake latency per client, in microseconds; -1 = never completed.
    private static long[] latencyMicros;

    /**
     * Usernames must be unique per RUN, not just per client. Reusing
     * "loaduser0..N" against a server that still holds registrations from the
     * previous run makes every connection bounce off the duplicate-name check
     * instantly — the run "passes" in milliseconds without testing anything.
     */
    private static final String RUN_ID = Long.toHexString(ProcessHandle.current().pid())
            + "-" + Long.toHexString(System.nanoTime() & 0xffffff);

    public static void main(String[] args) throws InterruptedException {
        parseArgs(args);
        latencyMicros = new long[clients];
        Arrays.fill(latencyMicros, -1);

        System.out.println("Firing " + clients + " connections at " + host + ":" + port
                + " (hold " + holdMillis + "ms, connect timeout " + connectTimeoutMillis
                + "ms, read timeout " + readTimeoutMillis + "ms)...");

        // Every thread waits on this latch, so the connections really do arrive as
        // one burst instead of trickling in as threads are created.
        CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[clients];

        for (int i = 0; i < clients; i++) {
            final int id = i;
            // Virtual threads: blocked-on-I/O generators cost almost nothing, so the
            // load test itself isn't the bottleneck at high client counts.
            threads[i] = Thread.ofVirtual().name("client-" + id).unstarted(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                attemptConnection(id);
            });
            threads[i].start();
        }

        long began = System.nanoTime();
        start.countDown();
        for (Thread t : threads)
            t.join(); // wait for all attempts to finish
        long elapsedMillis = (System.nanoTime() - began) / 1_000_000;

        report(elapsedMillis);
    }

    private static void attemptConnection(int id) {
        Socket socket = new Socket();
        try {
            long began = System.nanoTime();
            // Explicit connect with a deadline — the Socket(host, port) constructor
            // has none, so a lost SYN means minutes of kernel-level retries.
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);

            try (Socket s = socket;
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(s.getInputStream()));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

                // Server's first line is either the username prompt (accepted) or the
                // "Server is full" rejection message (refused by back-pressure).
                String firstLine = in.readLine();
                if (firstLine == null) {
                    recordFailure("closed before the prompt");
                    return;
                }
                if (firstLine.contains("full")) {
                    serverFull.incrementAndGet();
                    return;
                }

                // Accepted => complete the handshake with a username unique to this run.
                out.println("loaduser-" + RUN_ID + "-" + id);

                // Wait for the server's verdict. Other clients' join notices can arrive
                // in between, so read until we see one of the two outcomes.
                String line;
                boolean welcomed = false;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("Welcome,")) {
                        welcomed = true;
                        break;
                    }
                    if (line.contains("Username unavailable")) {
                        nameTaken.incrementAndGet();
                        return;
                    }
                }
                if (!welcomed) {
                    recordFailure("disconnected mid-handshake");
                    return;
                }

                latencyMicros[id] = (System.nanoTime() - began) / 1_000;
                connected.incrementAndGet();

                // Past this point the attempt has already been counted as connected, so
                // hold() records its own problems instead of throwing — otherwise one
                // client could be tallied twice and the totals wouldn't add up to N.
                hold(s, in); // keep the connection occupied
            }

        } catch (SocketTimeoutException e) {
            // Connect or read deadline expired: the far end never answered. The
            // classic cause is listen-backlog overflow, which strands the socket
            // in a connected-looking state that nobody is serving.
            timedOut.incrementAndGet();
            closeQuietly(socket);
        } catch (IOException e) {
            // ConnectException, resets => the connection didn't complete.
            recordFailure(e.getClass().getSimpleName() + ": " + e.getMessage());
            closeQuietly(socket);
        }
    }

    private static void recordFailure(String kind) {
        failed.incrementAndGet();
        failureKinds.computeIfAbsent(kind, k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * Holds the connection open for holdMillis while still draining whatever the
     * server sends. A real client reads continuously; one that only sleeps lets
     * its receive buffer fill, which would stall the server's broadcasts and
     * measure the load test's own rudeness rather than the server's capacity.
     */
    private static void hold(Socket socket, BufferedReader in) {
        long deadline = System.nanoTime() + holdMillis * 1_000_000L;
        try {
            while (true) {
                long remainingMillis = (deadline - System.nanoTime()) / 1_000_000;
                if (remainingMillis <= 0)
                    return;
                socket.setSoTimeout((int) remainingMillis);
                if (in.readLine() == null)
                    return; // server closed us
            }
        } catch (SocketTimeoutException e) {
            // Nothing more to read; the hold is simply over.
        } catch (IOException e) {
            // Reset mid-session: the server hung up on us (e.g. it dropped us as a
            // slow reader). Worth knowing about, but this client was already served.
            droppedDuringHold.incrementAndGet();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static void report(long elapsedMillis) {
        System.out.println("\n--- Results ---");
        System.out.println("Connected (served):        " + connected.get()
                + (droppedDuringHold.get() > 0
                        ? "   (" + droppedDuringHold.get() + " later dropped by the server)"
                        : ""));
        System.out.println("Refused (server full):     " + serverFull.get());
        System.out.println("Rejected (name taken):     " + nameTaken.get());
        System.out.println("Timed out (no response):   " + timedOut.get());
        System.out.println("Failed (error):            " + failed.get());
        failureKinds.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().get(), a.getValue().get()))
                .forEach(e -> System.out.println("    " + e.getValue().get() + "x  " + e.getKey()));

        int accountedFor = connected.get() + serverFull.get() + nameTaken.get()
                + timedOut.get() + failed.get();
        System.out.println("Total accounted for:       " + accountedFor + " / " + clients);
        System.out.println("Wall time:                 " + elapsedMillis + " ms");

        long[] done = Arrays.stream(latencyMicros).filter(v -> v >= 0).sorted().toArray();
        if (done.length > 0) {
            System.out.println("Handshake latency (ms):    p50=" + millis(percentile(done, 50))
                    + "  p95=" + millis(percentile(done, 95))
                    + "  max=" + millis(done[done.length - 1]));
        }
    }

    private static long percentile(long[] sorted, int p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }

    private static String millis(long micros) {
        return String.format("%.1f", micros / 1000.0);
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--host=")) {
                host = arg.substring("--host=".length());
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--clients=")) {
                clients = Integer.parseInt(arg.substring("--clients=".length()));
            } else if (arg.startsWith("--hold=")) {
                holdMillis = Integer.parseInt(arg.substring("--hold=".length()));
            } else if (arg.startsWith("--connect-timeout=")) {
                connectTimeoutMillis = Integer.parseInt(arg.substring("--connect-timeout=".length()));
            } else if (arg.startsWith("--read-timeout=")) {
                readTimeoutMillis = Integer.parseInt(arg.substring("--read-timeout=".length()));
            } else {
                System.out.println("Unknown option: " + arg);
                System.out.println("Usage: java LoadTest [--clients=N] [--hold=MS] [--host=H] [--port=N]"
                        + " [--connect-timeout=MS] [--read-timeout=MS]");
                System.exit(2);
            }
        }
    }
}
