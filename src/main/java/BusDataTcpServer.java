import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BusDataTcpServer {
    private static final int BATCH_SIZE = Integer.parseInt(
            System.getenv().getOrDefault("BUS_TCP_BATCH_SIZE", "512")
    );
    private static final long FLUSH_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("BUS_TCP_FLUSH_INTERVAL_MS", "200")
    );
    private static final long QUEUE_WARN_SIZE = Long.parseLong(
            System.getenv().getOrDefault("BUS_TCP_QUEUE_WARN_SIZE", "20000")
    );

    private final ServerSocket serverSocket;
    private final LinkedBlockingDeque<String> queue = new LinkedBlockingDeque<>();
    private final AtomicLong enqueuedMessages = new AtomicLong();
    private Socket clientSocket;
    private BufferedWriter writer;

    public BusDataTcpServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        System.out.println("TCP server started on port " + serverSocket.getLocalPort());
        startWriterThread();

        while (true) {
            Socket acceptedSocket = serverSocket.accept();
            acceptedSocket.setKeepAlive(true);

            synchronized (this) {
                closeClientConnection();
                clientSocket = acceptedSocket;
                writer = new BufferedWriter(new OutputStreamWriter(
                        clientSocket.getOutputStream(),
                        StandardCharsets.UTF_8
                ));
                notifyAll();
            }

            System.out.println("Spark connected to TCP server");
        }
    }

    public void sendData(String jsonData) {
        queue.offerLast(jsonData);

        long queueSize = queue.size();
        long sent = enqueuedMessages.incrementAndGet();
        if (queueSize >= QUEUE_WARN_SIZE && queueSize % 1000 == 0) {
            System.err.printf(
                    "TCP queue backlog is growing: queueSize=%d, enqueued=%d%n",
                    queueSize,
                    sent
            );
        }
    }

    private void startWriterThread() {
        Thread writerThread = new Thread(this::writerLoop, "bus-data-tcp-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void writerLoop() {
        List<String> batch = new ArrayList<>(BATCH_SIZE);
        while (true) {
            batch.clear();
            try {
                String first = queue.takeFirst();
                batch.add(first);
                queue.drainTo(batch, BATCH_SIZE - 1);

                BufferedWriter currentWriter = waitForWriter();
                writeBatch(currentWriter, batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                requeueBatch(batch);
                System.err.println("TCP send error: " + e.getMessage());
                synchronized (this) {
                    closeClientConnection();
                }
                sleepQuietly();
            }
        }
    }

    private BufferedWriter waitForWriter() throws InterruptedException {
        synchronized (this) {
            while (writer == null) {
                wait(TimeUnit.SECONDS.toMillis(1));
            }
            return writer;
        }
    }

    private static void writeBatch(BufferedWriter currentWriter, List<String> batch) throws IOException {
        for (String message : batch) {
            currentWriter.write(message);
            currentWriter.newLine();
        }
        currentWriter.flush();
    }

    private void requeueBatch(List<String> batch) {
        Collections.reverse(batch);
        for (String message : batch) {
            queue.offerFirst(message);
        }
    }

    private static void sleepQuietly() {
        try {
            Thread.sleep(FLUSH_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
