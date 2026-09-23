import java.net.*;
import java.io.*;

public class Client {
    private static String HOST = "localhost";
    private static int PORT = 5000;

    public static void main(String[] args) {
        parseArgs(args);

        // try-with-resources: sockets and streams auto-close on exit
        try (
                Socket socket = new Socket(HOST, PORT);
                BufferedReader serverIn = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                PrintWriter serverOut = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));) {

            // the client gets broadcast messages from other clients and input messages from
            // the user. Hence we will need to concurrently read messages from both the
            // sources.
            // the readerThread prints anything that the server sends its immediately even
            // when the main
            // thread blocks on the keyboard input
            Thread readerThread = new Thread(() -> {
                try {
                    String fromServer;
                    while ((fromServer = serverIn.readLine()) != null) {
                        System.out.println(fromServer);
                    }
                } catch (IOException e) {

                }
            });
            readerThread.setDaemon(true); // dies automatically when the app exits
            readerThread.start();

            // main thread: reads user input from the keyboard and sends it to the server so
            // that it is broadcasted
            String userInput;
            while ((userInput = keyboard.readLine()) != null) {
                serverOut.println(userInput);
                if (userInput.equalsIgnoreCase("BYE")) {
                    break;
                }
            }
        } catch (ConnectException e) {
            System.out.println("Could not connect to the server. Is the server running on port: " + PORT + " ?");
        } catch (UnknownHostException e) {
            System.out.println("Unknown host: " + HOST);
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }

        System.out.println("Disconnected from chat.");
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--host=")) {
                HOST = arg.substring("--host=".length());
            } else if (arg.startsWith("--port=")) {
                PORT = Integer.parseInt(arg.substring("--port=".length()));
            } else {
                System.out.println("Unknown option: " + arg);
                System.out.println("Usage: java Client [--host=H] [--port=N]");
                System.exit(2);
            }
        }
    }
}
