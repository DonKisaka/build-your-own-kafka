package com.example.build_your_own_kafka.broker;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a partition in Build Your Own Kafka,
 * containing segments of message logs.
 */
@Slf4j
public class Partition {

    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }


    private void initialize() {
        try {
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            File[] files = dir.listFiles((d, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName()
                            .substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);

                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        segments.add(new SegmentInfo(
                                baseOffset,
                                file.getAbsolutePath(),
                                indexFile.getAbsolutePath()
                        ));
                    }
                }

                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));

                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }

            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                openSegmentForAppend(segments.get(segments.size() - 1));
            }

            log.info("Initialized partition {} with {} segments, next offset: {}",
                    id, segments.size(), nextOffset.get());

        } catch (Exception e) {
            log.error("Failed to initialize partition {}", id, e);
        }
    }


    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(4);
            while (logChannel.position() < logChannel.size()) {
                buffer.clear();
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break;

                buffer.flip();
                int messageSize = buffer.getInt();
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }
        return count;
    }


    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;

        new File(logPath).createNewFile();
        new File(indexPath).createNewFile();

        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);
        openSegmentForAppend(segment);

        log.info("Created new segment for partition {}, base offset: {}", id, baseOffset);
    }


    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        if (activeLogFile != null) {
            activeLogFile.close();
        }

        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();
        activeLogChannel.position(activeLogChannel.size());
    }


    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();

            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            long position = activeLogChannel.position();
            activeLogChannel.write(buffer);
            activeLogChannel.force(true);

            updateIndex(currentOffset, position);
            nextOffset.incrementAndGet();

            return currentOffset;
        } catch (IOException e) {
            log.error("Failed to append message to partition {}", id, e);
            return -1;
        } finally {
            lock.writeLock().unlock();
        }
    }


    private void updateIndex(long offset, long position) {
        try {
            if (segments.isEmpty()) return;

            SegmentInfo currentSegment = segments.get(segments.size() - 1);

            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                 FileChannel indexChannel = indexFile.getChannel()) {

                indexChannel.position(indexChannel.size());

                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();

                indexChannel.write(buffer);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            log.error("Failed to update index for partition {}", id, e);
        }
    }


    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;

        try {
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) return messages;

            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0) return messages;

            try (RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                 FileChannel logChannel = logFile.getChannel()) {

                logChannel.position(position);
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                long currentOffset = offset;

                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    sizeBuffer.clear();
                    if (logChannel.read(sizeBuffer) < 4) break;

                    sizeBuffer.flip();
                    int messageSize = sizeBuffer.getInt();

                    if (bytesRead + messageSize > maxBytes) break;

                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    int messageRead = logChannel.read(messageBuffer);

                    if (messageRead < messageSize) {
                        log.warn("Incomplete message read at offset {}", currentOffset);
                        break;
                    }

                    messageBuffer.flip();
                    byte[] message = new byte[messageSize];
                    messageBuffer.get(message);
                    messages.add(message);

                    bytesRead += messageSize + 4;
                    currentOffset++;
                }
            }
        } catch (IOException e) {
            log.error("Failed to read messages from partition {}", id, e);
        } finally {
            lock.readLock().unlock();
        }

        return messages;
    }


    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) return null;

        int low = 0;
        int high = segments.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);

            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                if (offset >= segment.getBaseOffset()) return segment;
            }

            if (offset < segment.getBaseOffset()) high = mid - 1;
            else low = mid + 1;
        }

        return null;
    }


    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {

            if (indexChannel.size() == 0) return 0;

            long relativeOffset = offset - segment.getBaseOffset();
            long entryCount = indexChannel.size() / 16;

            if (relativeOffset >= entryCount) {
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                buffer.flip();
                buffer.getLong(); // skip offset
                return buffer.getLong();
            }

            indexChannel.position(relativeOffset * 16);
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();
            buffer.getLong(); // skip offset
            return buffer.getLong();

        } catch (IOException e) {
            log.error("Failed to find position for offset {}", offset, e);
            return -1;
        }
    }


    public int getId() { return id; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return new ArrayList<>(followers); }
    public void setFollowers(List<Integer> followers) { this.followers = new ArrayList<>(followers); }
    public long getLogEndOffset() { return nextOffset.get(); }


    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) activeLogChannel.close();
            if (activeLogFile != null) activeLogFile.close();
        } catch (IOException e) {
            log.error("Failed to close partition {} resources", id, e);
        } finally {
            lock.writeLock().unlock();
        }
    }



    @Getter
    @AllArgsConstructor
    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;
    }
}