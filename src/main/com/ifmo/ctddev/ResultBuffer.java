package com.ifmo.ctddev;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author korektur
 *         16/01/2017
 */
class ResultBuffer implements AutoCloseable {
    private final List<Long> buffer;
    private final BufferedWriter fileWriter;


    public ResultBuffer(File outFile) {
        Objects.requireNonNull(outFile);
        this.buffer = new ArrayList<>(ExternalSort.BUFFER_SIZE);
        try {
            this.fileWriter = new BufferedWriter(new PrintWriter(outFile));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public void add(long e) {
        buffer.add(e);
        if (buffer.size() == ExternalSort.BUFFER_SIZE) {
            flush();
        }
    }

    private void flush() {
        for (Long elem : buffer) {
            try {
                fileWriter.write(elem + System.lineSeparator());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        this.fileWriter.close();
    }
}
