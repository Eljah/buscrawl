import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class BusRawJsonSpool implements AutoCloseable {
    private static final DateTimeFormatter FILE_TIME_FORMAT = DateTimeFormatter
            .ofPattern("yyyyMMdd-HHmmss-SSS")
            .withZone(ZoneOffset.UTC);

    private final Path rootDir;
    private final Path openDir;
    private final Path readyDir;
    private final long rollIntervalMillis;
    private final long maxBytes;
    private final long forceSyncEveryMessages;
    private final String instanceId = UUID.randomUUID().toString().replace("-", "");

    private FileOutputStream outputStream;
    private BufferedWriter writer;
    private Path currentOpenFile;
    private long currentBytes;
    private long currentMessages;
    private long currentOpenedAtMillis;
    private long sequence;
    private volatile boolean closed;

    public BusRawJsonSpool(Path rootDir, Duration rollInterval, long maxBytes, long forceSyncEveryMessages)
            throws IOException {
        this.rootDir = rootDir;
        this.openDir = rootDir.resolve("open");
        this.readyDir = rootDir.resolve("ready");
        this.rollIntervalMillis = Math.max(1_000L, rollInterval.toMillis());
        this.maxBytes = Math.max(1024L, maxBytes);
        this.forceSyncEveryMessages = Math.max(0L, forceSyncEveryMessages);

        Files.createDirectories(openDir);
        Files.createDirectories(readyDir);
        recoverOpenFiles();
        startRollThread();
    }

    public static BusRawJsonSpool fromEnvironment(Path storageRoot) throws IOException {
        Path spoolRoot = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_DIR",
                storageRoot.resolve("raw-json-spool").toString()
        ));
        long rollSeconds = Long.parseLong(System.getenv().getOrDefault("BUS_RAW_SPOOL_ROLL_SECONDS", "10"));
        long maxBytes = Long.parseLong(System.getenv().getOrDefault("BUS_RAW_SPOOL_MAX_BYTES", "33554432"));
        long forceSyncEveryMessages = Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_FORCE_SYNC_EVERY_MESSAGES",
                "0"
        ));
        return new BusRawJsonSpool(spoolRoot, Duration.ofSeconds(rollSeconds), maxBytes, forceSyncEveryMessages);
    }

    public synchronized void append(String line) throws IOException {
        if (closed) {
            throw new IOException("Raw spool is closed");
        }

        rollIfNeeded(System.currentTimeMillis());
        if (writer == null) {
            openNewFile(System.currentTimeMillis());
        }

        writer.write(line);
        writer.newLine();
        writer.flush();

        currentMessages++;
        currentBytes += line.getBytes(StandardCharsets.UTF_8).length + 1L;
        if (forceSyncEveryMessages > 0 && currentMessages % forceSyncEveryMessages == 0) {
            outputStream.getChannel().force(false);
        }
    }

    public Path getReadyDir() {
        return readyDir;
    }

    public Path getRootDir() {
        return rootDir;
    }

    private void recoverOpenFiles() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(openDir, "*.jsonl.tmp")) {
            for (Path openFile : stream) {
                if (Files.size(openFile) == 0L) {
                    Files.deleteIfExists(openFile);
                    continue;
                }
                moveToReady(openFile);
            }
        }
    }

    private void startRollThread() {
        Thread rollThread = new Thread(() -> {
            while (!closed) {
                try {
                    Thread.sleep(1000L);
                    synchronized (this) {
                        rollIfNeeded(System.currentTimeMillis());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (IOException e) {
                    System.err.println("Raw spool roll error: " + e.getMessage());
                }
            }
        }, "bus-raw-json-spool-roller");
        rollThread.setDaemon(true);
        rollThread.start();
    }

    private void rollIfNeeded(long nowMillis) throws IOException {
        if (writer == null || currentMessages == 0L) {
            return;
        }
        if (nowMillis - currentOpenedAtMillis >= rollIntervalMillis || currentBytes >= maxBytes) {
            rollCurrentFile();
        }
    }

    private void openNewFile(long nowMillis) throws IOException {
        String fileName = String.format(
                "raw-%s-%s-%06d.jsonl.tmp",
                FILE_TIME_FORMAT.format(Instant.ofEpochMilli(nowMillis)),
                instanceId,
                sequence++
        );
        currentOpenFile = openDir.resolve(fileName);
        outputStream = new FileOutputStream(currentOpenFile.toFile(), true);
        writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        currentBytes = 0L;
        currentMessages = 0L;
        currentOpenedAtMillis = nowMillis;
    }

    private void rollCurrentFile() throws IOException {
        Path finishedFile = currentOpenFile;
        BufferedWriter finishedWriter = writer;
        FileOutputStream finishedOutputStream = outputStream;

        writer = null;
        outputStream = null;
        currentOpenFile = null;
        currentBytes = 0L;
        currentMessages = 0L;

        finishedWriter.flush();
        finishedOutputStream.getChannel().force(false);
        finishedWriter.close();
        moveToReady(finishedFile);
    }

    private void moveToReady(Path openFile) throws IOException {
        String readyFileName = openFile.getFileName().toString().replaceFirst("\\.tmp$", "");
        Path readyFile = readyDir.resolve(readyFileName);
        try {
            Files.move(openFile, readyFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicMoveFailed) {
            Files.move(openFile, readyFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (writer != null) {
            if (currentMessages > 0L) {
                rollCurrentFile();
            } else {
                Path emptyFile = currentOpenFile;
                writer.close();
                writer = null;
                outputStream = null;
                currentOpenFile = null;
                if (emptyFile != null) {
                    Files.deleteIfExists(emptyFile);
                }
            }
        }
    }
}
