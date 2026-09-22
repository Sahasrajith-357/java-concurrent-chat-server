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
 */
public class Server {

    private static final int PORT = 5000;

    // EXECUTOR STRATEGY: They are alternatives, not combinable.
    // true -> virtual threads: one per client, ~unlimited, ideal for blocking I/O.
    // false -> bounded platform-thread pool: capped concurrency + real
    // back-pressure.
    private static final boolean USE_VIRTUAL_THREADS = true;

    // Bounded-pool settings:
    private static final int CORE_POOL = 4; // threads kept alive at idle
    private static final int MAX_POOL = 10; // ceiling on worker threads
    private static final int QUEUE_CAPACITY = 20; // tasks that can wait before we reject

    /**
     * THE SHARED REGISTRY: username -> client's output stream.
     * ConcurrentHashMap:
     * - Keying by username gives O(1) lookup for direct messages (a list can't).
     * - It's thread-safe under concurrent joins/leaves/lookups with fine-grained
     * internal locking — no global lock, so clients don't block each other.
     * - Its iterators are weakly consistent (fail-safe): broadcasting by iterating
     * the map never throws even while another thread adds/removes a client.
     */
    private static final ConcurrentHashMap<String, PrintWriter> clients = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ExecutorService pool = createExecutor();
        System.out.println("Server on port " + PORT + " using "
                + (USE_VIRTUAL_THREADS ? "virtual threads" : "bounded thread pool"));

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket socket = serverSocket.accept(); // main thread: accept only
                try {
                    pool.submit(new ClientHandler(socket));
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
            pool.shutdown();
        }
    }

    // Builds the chosen executor. This is the one place the two strategies diverge.
    private static ExecutorService createExecutor() {
        if (USE_VIRTUAL_THREADS) {
            // One virtual thread per task. Virtual threads are lightweight and don't
            // map 1:1 to OS threads, so hundreds of thousands of clients blocked in
            // readLine() cost almost nothing. No bounding needed or wanted.
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        // Bounded pool: up to MAX_POOL workers, with a BOUNDED queue in front.
        // When both are full, AbortPolicy throws RejectedExecutionException — the
        // explicit back-pressure that Executors.newFixedThreadPool (unbounded queue)
        // silently lacks.
        return new ThreadPoolExecutor(
                CORE_POOL, MAX_POOL,
                60L, TimeUnit.SECONDS, // idle workers above core die after 60s
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadPoolExecutor.AbortPolicy());
        // Note: AbortPolicy — NOT CallerRunsPolicy — is correct for a server.
        // CallerRuns would execute the handler on the MAIN thread, blocking the accept
        // loop and
        // freezing all new connections. We want to reject, not stall accepting.
    }

    // Tell a refused client the server is full, then close.
    private static void rejectConnection(Socket socket) {
        try (Socket s = socket) {
            new PrintWriter(s.getOutputStream(), true)
                    .println("[Server] Server is full. Please try again later.");
        } catch (IOException ignored) {
        }
    }

    // Handles one client's whole session. Runnable, so it runs on either executor
    private static class ClientHandler implements Runnable {

        private final Socket socket;
        private String username; // null until the handshake succeeds
        private PrintWriter out;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (Socket clientSocket = this.socket;
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {

                this.out = writer;

                // USERNAME HANDSHAKE
                out.println("Enter a username:");
                String name = in.readLine();
                if (name == null)
                    return; // left during handshake
                name = name.trim();

                // putIfAbsent is ATOMIC: it registers the name only if free, and
                // reports failure otherwise in one operation.
                if (name.isEmpty() || clients.putIfAbsent(name, out) != null) {
                    out.println("[Server] Username unavailable. Disconnecting.");
                    return;
                }
                this.username = name;

                broadcast("[Server] " + username + " joined the chat.", username);
                out.println("Welcome, " + username
                        + "! Use '@user message' for a direct message, or 'BYE' to leave.");

                // SESSION LOOP
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

            } catch (SocketException e) {
                System.out.println("Client disconnected abruptly.");
            } catch (IOException e) {
                System.out.println("Connection error: " + e.getMessage());
            } finally {
                // Always deregister so we never broadcast to a dead connection.
                if (username != null) {
                    clients.remove(username);
                    broadcast("[Server] " + username + " left the chat.", username);
                }
            }
        }

        // Send to everyone except the sender. Iterating the map is fail-safe.
        private void broadcast(String message, String sender) {
            for (var entry : clients.entrySet()) {
                if (!entry.getKey().equals(sender)) {
                    entry.getValue().println(message);
                }
            }
        }

        // Handle "@username message" O(1) lookup by key
        private void sendDirect(String line) {
            int space = line.indexOf(' ');
            if (space == -1) {
                out.println("[Server] Usage: @username message");
                return;
            }

            String target = line.substring(1, space);
            String message = line.substring(space + 1);
            PrintWriter targetOut = clients.get(target); // O(1) key lookup

            if (targetOut == null) {
                out.println("[Server] User '" + target + "' not found.");
            } else {
                targetOut.println("[DM from " + username + "] " + message);
                out.println("[DM to " + target + "] " + message);
            }
        }
    }
}
