import java.io.*;
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
        System.out.println("🟢 TCP-сервер запущен на порту " + serverSocket.getLocalPort());
        clientSocket = serverSocket.accept();
        writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        System.out.println("✅ Spark подключился к TCP-серверу");
    }

    public synchronized void sendData(String jsonData) {
        try {
            writer.write(jsonData);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("❌ Ошибка отправки данных по TCP: " + e.getMessage());
        }
    }
}
