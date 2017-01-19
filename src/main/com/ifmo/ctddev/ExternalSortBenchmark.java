package com.ifmo.ctddev;

import org.openjdk.jmh.annotations.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author korektur
 *         16/01/2017
 */
public class ExternalSortBenchmark {
    private static final Random RANDOM = new Random();
    private static final int FILE_SIZE = 1000000;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private static final File IN_FILE = new File("resources/test/in");

        @Setup(Level.Trial)
        public void generateTestInputData() throws Exception {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(IN_FILE))) {
                RANDOM.longs(FILE_SIZE)
                        .forEach(elem -> {
                            try {
                                writer.write(elem + System.lineSeparator());
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        });
            }
        }
    }

    @Benchmark()
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(value = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void sort(BenchmarkState benchmarkState) throws Exception {
        ExternalSort.sort(benchmarkState.IN_FILE, new File("resources/test/out.txt"),
                "resources/test/");
    }
}
