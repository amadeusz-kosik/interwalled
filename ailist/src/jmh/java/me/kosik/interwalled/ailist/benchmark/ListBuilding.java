package me.kosik.interwalled.ailist.benchmark;

import me.kosik.interwalled.ailist.AIList;
import me.kosik.interwalled.ailist.AIListBuilder;
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
public class ListBuilding {

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
        "102400",
        "204800"
    })
    public String databaseRowsCount;

    private final Map<String, ArrayList<Interval<String>>> dataSources = new HashMap<>();

    @Setup
    public void setup() {
        DataSources.initializeDatabaseSources(databaseRowsCount, dataSources);
    }

    @Benchmark
    public void benchmarkBulkLoad(final Blackhole blackhole) {
        AIListBuilder<String> aiListBuilder =
            new AIListBuilder<>(AIListConfiguration.DEFAULT, dataSources.get(dataSource));

        AIList<String> aiList = aiListBuilder.build();
        blackhole.consume(aiList);
    }

    @Benchmark
    public void benchmarkIterativeLoad(final Blackhole blackhole) {
        AIListBuilder<String> aiListBuilder =
            new AIListBuilder<>(AIListConfiguration.DEFAULT);

        for(Interval<String> interval : dataSources.get(dataSource))
            aiListBuilder.put(interval);

        AIList<String> aiList = aiListBuilder.build();
        blackhole.consume(aiList);
    }
}
