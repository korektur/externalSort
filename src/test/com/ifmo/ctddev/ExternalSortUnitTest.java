package com.ifmo.ctddev;

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author korektur
 *         16/01/2017
 */
public class ExternalSortUnitTest {

    private static final File IN_FILE = new File("resources/test/in");
    private static final Random random = new Random();

    public List<Long> generateTestInputData(int fileSize) throws InterruptedException, IOException {
        List<Long> data = new ArrayList<>(fileSize);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(IN_FILE))) {
            random
                    .longs(fileSize, 1, 10)
                    .forEach(elem -> {
                        try {
                            data.add(elem);
                            writer.write(elem + System.lineSeparator());
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
        }
        return data;
    }

    private void checkOutFile(File outFile, List<Long> data) throws IOException {
        Collections.sort(data);
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(outFile))) {
            for (Long expected : data) {
                Long actual = Long.parseLong(bufferedReader.readLine());
                assertEquals(expected, actual);
            }
            assertNull(bufferedReader.readLine());
        }
    }

    private void runtTest(int bufferSize, int filesLimit, int chunkSize, int fileSize) throws Exception {
        ExternalSort.BUFFER_SIZE = bufferSize;
        ExternalSort.FILES_LIMIT = filesLimit;
        ExternalSort.CHUNK_SIZE = chunkSize;
        List<Long> longs = generateTestInputData(fileSize);
        File outputFile = new File("resources/test/out.txt");
        ExternalSort.sort(IN_FILE, outputFile, "resources/test/");
        checkOutFile(outputFile, longs);
    }

    @Test
    public void sortDefault() throws Exception {
        runtTest(4096, 200, 4000, 10000);
    }

    @Test
    public void sortSmallBuffer() throws Exception {
        runtTest(100, 200, 4000, 10000);
    }


    @Test
    public void sortSmallFileLimit() throws Exception {
        runtTest(4096, 2, 4000, 10000);
    }


    @Test
    public void sortDefaultSmallChunkSize() throws Exception {
        runtTest(4096, 200, 100, 10000);
    }

    @Test
    public void sortDefaultBigFile() throws Exception {
        runtTest(4096, 200, 4000, 1000000);
    }
}
