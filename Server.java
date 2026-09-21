import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;

public class Server {
    public static void main(String[] args) {
        try (ServerSocket listener = new ServerSocket(5000)) {
            System.out.println("Server is listening on port 5000");
            while (true) {
                try (Socket connection = listener.accept()) {
                    System.out.println("Client connected: " + connection.getInetAddress().getHostAddress());
                    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    PrintWriter out = new PrintWriter(connection.getOutputStream(), true);
                    String message;
                    while ((message = in.readLine()) != null) {
                        System.out.println("Received: " + message);
                        out.println("Echo: " + message);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
