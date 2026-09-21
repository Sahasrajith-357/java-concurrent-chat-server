import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;

public class Server {

    static void handle(Socket connection) throws IOException {
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
    }

    public static void main(String[] args) {
        try (ServerSocket listener = new ServerSocket(5000)) {
            System.out.println("Server is listening on port 5000");
            while (true) {
                try (Socket connection = listener.accept()) {
                    handle(connection);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Server is shutting down.");
        }
    }
}
