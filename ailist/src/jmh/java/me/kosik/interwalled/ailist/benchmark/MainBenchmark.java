package me.kosik.interwalled.ailist.benchmark;

import me.kosik.interwalled.ailist.AIList;
import me.kosik.interwalled.ailist.AIListBuilder;
import me.kosik.interwalled.ailist.AIListIterator;
import me.kosik.interwalled.ailist.DataSources;
import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
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

    @Param({
        "1_000_0000",
        "1_500_0000",
        "2_000_0000",
        "2_500_0000",
        "3_000_0000",
        "3_500_0000",
        "4_000_0000",
        "4_500_0000",
        "5_000_0000"
    })
    public String databaseRowsCount;

    @Param({
        "querySparse",
        "queryDense"
    })
    public String querySource;

    @Param({
         "50_000",
        "500_000"
    })
    public String queryRowsCount;

    private final Map<String, ArrayList<Interval<String>>> dataSources = new HashMap<>();
    private final Map<String, ArrayList<Interval<String>>> querySources = new HashMap<>();

    @Setup
    public void setup() {
        DataSources.initializeDatabaseSources(databaseRowsCount, dataSources);
        DataSources.initializeQuerySources(databaseRowsCount, queryRowsCount, querySources);
    }

    @Benchmark
    public void benchmarkJoin(final Blackhole blackhole) {
        AIListBuilder<String> aiListBuilder =
            new AIListBuilder<>(AIListConfiguration.DEFAULT, dataSources.get(dataSource));

        AIList<String> aiList = aiListBuilder.build();

        for(Interval<String> interval: querySources.get(querySource)) {
            for (AIListIterator<String> it = aiList.overlapping(interval); it.hasNext(); ) {
                Interval<String> dbInterval = it.next();
                blackhole.consume(dbInterval);
            }
        }
    }

}
