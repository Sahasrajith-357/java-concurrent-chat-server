import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArrayList;

public class Server {
    // the port the server listens on. Ports below 1024 are reserved for system use
    // and require elevated privileges to bind to.
    private static final int PORT = 5000;

    // Bounded concurrency: at most these many clients are handled at once.
    // extra connections will be queued until a thread is available.
    private static final int THREAD_POOL_SIZE = 10;

    private static final CopyOnWriteArrayList<PrintWriter> clientWriters = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        System.out.println("Starting the server on port " + PORT + "...");

        // a fixed size thread pool to handle client connections concurrently
        // we submit each client session to the pool instead of doing new
        // Thread(handler).start() to avoid creating too many threads and overwhelming
        // the system.
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // try-with-resources to automatically close the ServerSocket when we leave this
        // block
        try (ServerSocket listener = new ServerSocket(PORT)) {

            // the main thread's only responsibility
            while (true) {
                // accept() blocks until a client connects, then returns a new Socket for that
                // client. The main thread then hands off the Socket to a new ClientHandler
                // this is literally the only thing the main thread does
                Socket connection = listener.accept();
                System.out.println("New connection from " + connection.getInetAddress());

                // hand the connection to the pool. the worker runs the whole session
                // the main thread loops back to accept a new connection
                pool.submit(new ClientHandler(connection));
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        } finally {
            // release the worker threads on shutdown
            pool.shutdown();
            System.out.println("Server is shutting down.");
        }
    }

    // handles one client's entire session. Done by implementing Runnable instead of
    // extending Thread
    // so that the task is decoupled from the thread management lifecycle
    // and to preserve the single inheritance slot for other functionalities
    private static class ClientHandler implements Runnable {

        private final Socket socket; // the handler's own private connection
        private PrintWriter out; // the client's output stream, stored in the shared registry
        private final int clientPort; // each client gets assigned a unique port to identify its socket
        private final String HOST; // the ip address of the localhost

        public ClientHandler(Socket socket) {
            this.socket = socket;
            this.clientPort = socket.getPort();
            this.HOST = socket.getInetAddress().toString();
        }

        @Override
        public void run() {

            // try-with-resources: automatically closes the sockets, input and output
            // streams on leaving this block
            try (
                    Socket clientSocket = this.socket;
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); // to
                                                                                                                  // read
                                                                                                                  // the
                                                                                                                  // client's
                                                                                                                  // messages
                    PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);) { // to reply to the
                                                                                                   // client's messages
                this.out = writer;
                clientWriters.add(out); // add the client's output stream to the shared registry for broadcasting
                                        // messages to all the clients
                String clientName = Thread.currentThread().getName(); // worker id
                broadcast("[Server] A new user " + HOST + ":" + clientPort + " has joined the chat.", out);
                out.println("Hey Hey Heyyy! Welcome to chat. Enter your messages. Type BYE to leave.");
                System.out.println(clientName + " is now handling a client.");

                String line;
                // session loop: reads messages sent by client until the client leaves
                // readLine() blocks until a full line arrives and returns null when a client
                // closes the connection
                while ((line = in.readLine()) != null) {
                    if (line.equalsIgnoreCase("bye")) {
                        out.println("Goodbye!");
                        break;
                    }

                    System.out.println("Received: " + line);

                    // broadcast the message to all every other connected client
                    broadcast(clientPort + ": " + line, out);
                }
            } catch (SocketException e) {
                System.out.println("A client disconnected abruptly.");
            } catch (IOException e) {
                System.out.println("Connection error: " + e.getMessage());
            } finally {
                if (out != null) {
                    clientWriters.remove(out);
                    broadcast("[Server] user " + clientPort + " left the chat.", out);
                }
                System.out.println("Active session ended. Active clients: " + clientWriters.size());
            }
        }
    }

    // sends the message to every client connected to the server except itself
    // CopyOnWriteArrayList lets us iterate it using a forEach loop while client's
    // leave or join
    // since it operates only on a stable snapshot of the List.
    private static void broadcast(String message, PrintWriter sender) {
        for (PrintWriter writer : clientWriters) {
            if (writer != sender) {
                writer.println(message);
            }
        }
    }
}
