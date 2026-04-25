package me.kosik.interwalled.ailist.core.benchmark;


import me.kosik.interwalled.ailist.core.AIList;
import me.kosik.interwalled.ailist.core.AIListBuilder;
import me.kosik.interwalled.ailist.core.DataSources;
import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ListBuildingBenchmark {

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

    @Param({ "100" })
    public int databaseRowsCount;

    protected final Map<String, ArrayList<Interval>> dataSources = new HashMap<>();

    @Setup
    public void setup() {
        DataSources.initializeDatabaseSources(databaseRowsCount, dataSources);
    }

    @Benchmark
    public void benchmarkJoin(final Blackhole blackhole) {
        AIListBuilder aiListBuilder = new AIListBuilder(Configuration.DEFAULT);
        List<AIList> aiLists = aiListBuilder.build(dataSources.get(dataSource));

        for(AIList aiList: aiLists)
            blackhole.consume(aiList);
    }
}
