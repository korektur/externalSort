package com.ifmo.ctddev;

import org.openjdk.jmh.annotations.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author korektur
 *         16/01/2017
 */
//@RunWith(JUnit4.class)
public class ExternalSortTest {

    private static final String IN_FILE_PATH = "resources/test/in";
    private static final int FILE_SIZE = 4;
    private static final int FILES_COUNT = 10;
    private static final ThreadLocal<Random> random = ThreadLocal.withInitial(Random::new);
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4,
            10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100));

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private List<File> files;

        @Setup(Level.Trial)
        public void generateTestInputData() throws InterruptedException {
            files = new ArrayList<>(FILES_COUNT);
            for (int i = 0; i < FILES_COUNT; i++) {
                File file = new File(IN_FILE_PATH + i);
                executor.submit(new TestDataGenerator(file));
                files.add(file);

            }
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static class TestDataGenerator implements Runnable {
        private final File file;

        private TestDataGenerator(File file) {
            this.file = Objects.requireNonNull(file);
        }

        @Override
        public void run() {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                random.get()
                        .longs(FILE_SIZE, 1, 10)
                        .sorted()
                        .forEach(elem -> {
                            try {
                                writer.write(elem + System.lineSeparator());
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        });

            } catch (IOException e) {
                throw new IllegalStateException();
            }
        }
    }



    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    @Fork(value = 5)
    public void sort(BenchmarkState benchmarkState) throws Exception {
       ExternalSort.sort(benchmarkState.files, new File("resources/test/out.txt"));
    }
}
