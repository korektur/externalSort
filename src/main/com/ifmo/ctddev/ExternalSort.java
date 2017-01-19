package com.ifmo.ctddev;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author korektur
 *         16/01/2017
 */
public class ExternalSort {

    static int BUFFER_SIZE = 4096;
    static int FILES_LIMIT = 200;
    static int CHUNK_SIZE = 4000;

    private static void writeChunk(List<Long> data, Writer writer) throws IOException {
        Collections.sort(data);
        for (Long elem : data) {
            writer.write(elem + System.lineSeparator());
        }
    }

    public static void sort(File inputFile, File outputFile, String tempDirectory) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        List<File> files = new ArrayList<>();
        String line;
        int fileCount = 0;
        File curFile = new File(tempDirectory + "temp" + (++fileCount));
        BufferedWriter chunkWriter = new BufferedWriter(new FileWriter(curFile));
        List<Long> curChunk = new ArrayList<>(CHUNK_SIZE);
        while((line = bufferedReader.readLine()) != null) {
            curChunk.add(Long.parseLong(line));
            if (curChunk.size() == CHUNK_SIZE) {
                writeChunk(curChunk, chunkWriter);
                chunkWriter.close();
                files.add(curFile);

                if (files.size() >= FILES_LIMIT) {
                    File newChunk = new File(tempDirectory + "temp" + (++fileCount));
                    sort(files, newChunk);
                    files.clear();
                    files.add(newChunk);
                }

                curFile = new File(tempDirectory + "temp" + (++fileCount));
                chunkWriter = new BufferedWriter(new FileWriter(curFile));
                curChunk = new ArrayList<>(CHUNK_SIZE);
            }
        }

        if (curChunk.size() > 0) {
            writeChunk(curChunk, chunkWriter);
            chunkWriter.close();
            files.add(curFile);
        }

        sort(files, outputFile);
    }

    private static void sort(Collection<File> chunkFilePaths, File resultFilePath) throws Exception {
        List<ExternalArray> collect = chunkFilePaths.stream()
                .map(ExternalArray::new)
                .collect(Collectors.toList());
        PriorityQueue<ExternalArray> queue = new PriorityQueue<>(collect);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFilePath))) {

            while (!queue.isEmpty()) {
                ExternalArray cur = queue.poll();

                while (!cur.isEmpty() && (queue.isEmpty() || cur.compareTo(queue.peek()) <= 0)) {
                    Long poll = cur.poll();
                    writer.write(poll + System.lineSeparator());
                }

                if (!cur.isEmpty()) {
                    queue.add(cur);
                } else {
                    cur.close();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            for (ExternalArray externalArray : queue) {
                externalArray.close();
            }
        }
    }

}
