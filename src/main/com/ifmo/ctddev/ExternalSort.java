package com.ifmo.ctddev;

import java.io.*;
import java.util.*;

/**
 * @author korektur
 *         16/01/2017
 */
public class ExternalSort {

    static final int BUFFER_SIZE = 4096;

    public static void sort(Collection<File> chunkFilePaths, File resultFilePath) throws Exception {
        PriorityQueue<ExternalArray> queue = chunkFilePaths.stream()
                .map(ExternalArray::new)
                .collect(PriorityQueue::new, PriorityQueue::add, AbstractQueue::addAll);

        try (ResultBuffer resultBuffer = new ResultBuffer(resultFilePath)) {

            while (!queue.isEmpty()) {
                ExternalArray cur = queue.poll();

                while (!cur.isEmpty() && (queue.isEmpty() || cur.compareTo(queue.peek()) <= 0)) {
                    resultBuffer.add(cur.poll());
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
