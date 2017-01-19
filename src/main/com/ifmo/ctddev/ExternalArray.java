package com.ifmo.ctddev;

import com.sun.istack.internal.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

/**
 * @author korektur
 *         16/01/2017
 */
class ExternalArray implements AutoCloseable, Comparable<ExternalArray> {

    private final BufferedReader reader;
    private Queue<Long> buffer;
    private boolean ended;

    ExternalArray(File file) {
        Objects.requireNonNull(file);
        this.ended = false;
        try {
            this.reader = new BufferedReader(new FileReader(file));
            buffer = fillBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Queue<Long> fillBuffer() throws IOException {
        String line = null;
        Queue<Long> queue = new ArrayDeque<>(ExternalSort.BUFFER_SIZE);
        while (!ended && queue.size() < ExternalSort.BUFFER_SIZE && (line = reader.readLine()) != null) {
            queue.add(Long.parseLong(line));
        }

        this.ended = line == null;
        return queue;
    }

    @Nullable
    Long poll() throws IOException {
        if (!ended && buffer.isEmpty()) {
            buffer = fillBuffer();
        }
        return buffer.poll();
    }

    boolean isEmpty() throws IOException {
        if (!ended && buffer.isEmpty()) {
            buffer = fillBuffer();
        }
        return buffer.isEmpty();
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    @Override
    public int compareTo(ExternalArray o) {
        Objects.requireNonNull(o);
        if (buffer.isEmpty() || o.buffer.isEmpty()) {
            return -Integer.compare(buffer.size(), o.buffer.size());
        }
        return buffer.peek().compareTo(o.buffer.peek());
    }
}
