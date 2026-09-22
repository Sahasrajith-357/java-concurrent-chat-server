import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ClientHandler implements Runnable {
    private Socket connection;

    public ClientHandler(Socket connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        try {
            System.out.println("Handling client: " + Thread.currentThread().getName());
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            PrintWriter out = new PrintWriter(connection.getOutputStream(), true);
            String message;
            System.out.println("Client connected: " + connection.getInetAddress().getHostAddress());
            while ((message = in.readLine()) != null) {
                if (message.equalsIgnoreCase("exit")) {
                    System.out.println("Client requested to close the connection.");
                    break;
                }
                System.out.println("Received: " + message);
                out.println("Echo: " + message);
            }
            in.close();
            out.close();
            System.out.println("Client disconnected: " + connection.getInetAddress().getHostAddress());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

public class Server {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (ServerSocket listener = new ServerSocket(5000)) {
            System.out.println("Server is listening on port 5000");
            while (true) {
                Socket connection = listener.accept();
                ClientHandler handler = new ClientHandler(connection);
                executor.submit(handler);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
            System.out.println("Server is shutting down.");
        }
    }
}
