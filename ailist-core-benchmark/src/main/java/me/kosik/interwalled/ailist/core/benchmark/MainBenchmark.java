package me.kosik.interwalled.ailist.core.benchmark;


import me.kosik.interwalled.ailist.core.AIList;
import me.kosik.interwalled.ailist.core.AIListBuilder;
import me.kosik.interwalled.ailist.core.AIListIterator;
import me.kosik.interwalled.ailist.core.BenchmarkData;
import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;



@State(Scope.Benchmark)
public class MainBenchmark {

    @Param({
            "consecutiveIntervals",
            "overlappingIntervals",
            "lastingIntervals",
            "shortPoissonIntervals",
            "mixed1Intervals",
            "mixed2Intervals",
            "mixed3Intervals",
            "mixed4Intervals"
    })
    public String dataSource;

    @Param({ "querySparse", "queryDense" })
    public String querySource;

    @Param({ "100000" })
    public int databaseRowsCount;

    @Param({ "100" })
    public int queryRowsCount;

    protected final Map<String, ArrayList<Interval>> dataSources = new HashMap<>();
    protected final Map<String, ArrayList<Interval>> querySources = new HashMap<>();

    @Setup
    public void setup() {
        BenchmarkData.initializeDatabaseSources(databaseRowsCount, dataSources);
        BenchmarkData.initializeQuerySources(databaseRowsCount, queryRowsCount, querySources);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(1)
    @Warmup(        iterations =  3, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(   iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void intervalJoinBenchmark(final Blackhole blackhole) {
        ArrayList<Interval> inputData = (ArrayList<Interval>) dataSources.get(dataSource).clone();

        AIListBuilder aiListBuilder = new AIListBuilder(Configuration.DEFAULT);
        List<AIList> aiLists = aiListBuilder.build(inputData);

        for(AIList aiList: aiLists)
            for(Interval interval: querySources.get(querySource)) {
                for (AIListIterator it = aiList.overlapping(interval); it.hasNext(); ) {
                    Interval dbInterval = it.next();
                    blackhole.consume(dbInterval);
                }
            }
    }

    public static void main(String[] args) throws Exception {
        String fullClassName = MainBenchmark.class.getName();
        org.openjdk.jmh.Main.main(new String[] { fullClassName + ".intervalJoinBenchmark" });
    }
}
