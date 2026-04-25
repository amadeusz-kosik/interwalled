package me.kosik.interwalled.ailist.core.benchmark;


import me.kosik.interwalled.ailist.core.AIList;
import me.kosik.interwalled.ailist.core.AIListBuilder;
import me.kosik.interwalled.ailist.core.AIListIterator;
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
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
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

    @Param({ "1000000"  })
    public int databaseRowsCount;

    @Param({ "5000" })
    public int queryRowsCount;

    protected final Map<String, ArrayList<Interval>> dataSources = new HashMap<>();
    protected final Map<String, ArrayList<Interval>> querySources = new HashMap<>();

    @Setup
    public void setup() {
        DataSources.initializeDatabaseSources(databaseRowsCount, dataSources);
        DataSources.initializeQuerySources(databaseRowsCount, queryRowsCount, querySources);
    }

//    @Benchmark
//    public void benchmarkJoin(final Blackhole blackhole) {
//        AIListBuilder aiListBuilder = new AIListBuilder(Configuration.DEFAULT);
//        List<AIList> aiLists = aiListBuilder.build(dataSources.get(dataSource));
//
//        for(AIList aiList: aiLists)
//            for(Interval interval: querySources.get(querySource)) {
//                for (AIListIterator it = aiList.overlapping(interval); it.hasNext(); ) {
//                    Interval dbInterval = it.next();
//                    blackhole.consume(dbInterval);
//                }
//            }
//    }
}
