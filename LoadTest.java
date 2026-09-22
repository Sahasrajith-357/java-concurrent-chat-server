import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Load generator for ChatServer. Opens CLIENTS connections concurrently, each
 * on its own thread, does the username handshake, holds the connection for a
 * moment, then closes. Prints how many connected / were refused / failed.
 */
public class LoadTest {

    private static final String HOST = "localhost";
    private static final int PORT = 5000;
    private static final int CLIENTS = 1000; // how many simultaneous connections to attempt
    private static final int HOLD_MILLIS = 300; // keep each connection open this long

    // Thread-safe counters: many generator threads increment these at once.
    private static final AtomicInteger connected = new AtomicInteger();
    private static final AtomicInteger refused = new AtomicInteger();
    private static final AtomicInteger failed = new AtomicInteger();

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Firing " + CLIENTS + " connections at " + HOST + ":" + PORT + "...");
        Thread[] threads = new Thread[CLIENTS];

        for (int i = 0; i < CLIENTS; i++) {
            final int id = i;
            threads[i] = new Thread(() -> attemptConnection(id));
            threads[i].start();
        }
        for (Thread t : threads)
            t.join(); // wait for all attempts to finish

        System.out.println("\n--- Results ---");
        System.out.println("Connected (served): " + connected.get());
        System.out.println("Refused (server full): " + refused.get());
        System.out.println("Failed (error/timeout): " + failed.get());
    }

    private static void attemptConnection(int id) {
        try (Socket socket = new Socket(HOST, PORT);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Server's first line is either the username prompt (accepted) or the
            // "Server is full" rejection message (refused by back-pressure).
            String firstLine = in.readLine();
            if (firstLine != null && firstLine.contains("full")) {
                refused.incrementAndGet();
                return;
            }

            // Accepted => complete the handshake with a unique username and hold.
            out.println("loaduser" + id);
            connected.incrementAndGet();
            Thread.sleep(HOLD_MILLIS); // keep the connection occupied

        } catch (IOException | InterruptedException e) {
            // ConnectException, resets, timeouts => the connection didn't complete.
            failed.incrementAndGet();
        }
    }
}
