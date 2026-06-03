import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BusDataTcpServer {
    private final ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedWriter writer;

    public BusDataTcpServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        System.out.println("TCP server started on port " + serverSocket.getLocalPort());

        while (true) {
            Socket acceptedSocket = serverSocket.accept();
            acceptedSocket.setKeepAlive(true);

            synchronized (this) {
                closeClientConnection();
                clientSocket = acceptedSocket;
                writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            }

            System.out.println("Spark connected to TCP server");
        }
    }

    public synchronized void sendData(String jsonData) {
        if (writer == null) {
            System.err.println("Waiting for Spark to connect...");
            return;
        }

        try {
            writer.write(jsonData);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("TCP send error: " + e.getMessage());
            closeClientConnection();
        }
    }

    private synchronized void closeClientConnection() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException ignored) {
            }
            writer = null;
        }

        if (clientSocket != null) {
            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
            clientSocket = null;
        }
    }
}
