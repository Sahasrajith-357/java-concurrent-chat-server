import java.net.*;
import java.io.*;


class Client {
    public static void main(String[] args) {
        try (Socket connection = new Socket("localhost", 5000)) {
            PrintWriter out = new PrintWriter(connection.getOutputStream(), true);
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String userInput;
            while ((userInput = stdin.readLine()) != null) {
                if (userInput.equalsIgnoreCase("exit")) {
                    System.out.println("Exiting client.");
                    break;
                }
                out.println(userInput);
                String response = in.readLine();
                if (response == null) {
                    System.out.println("Server closed the connection.");
                    break;
                }
                System.out.println("Server response: " + response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
