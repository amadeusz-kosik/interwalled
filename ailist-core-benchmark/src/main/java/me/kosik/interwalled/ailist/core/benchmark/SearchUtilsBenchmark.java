package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.model.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
public class SearchUtilsBenchmark {

    final static int INTERVALS_COUNT = 256000;

    final ArrayList<Interval> intervals = new ArrayList<>(INTERVALS_COUNT);

    @Param({ "3", "7", "15", "31", "63" })
    int cutoff;

    @Setup
    public void setup() {
        for(int i = 0; i < INTERVALS_COUNT; ++ i) {
            intervals.add(new Interval(i, i + 1, (long) i));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @Warmup(        iterations =  3, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(   iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchmark(Blackhole blackhole) {
        blackhole.consume(SearchUtils.findRightmost(intervals, 1000L, cutoff));
    }

    public static void main(String[] args) throws Exception {
        String fullClassName = SearchUtilsBenchmark.class.getName();
        org.openjdk.jmh.Main.main(new String[] { fullClassName + ".benchmark" });
    }
}
