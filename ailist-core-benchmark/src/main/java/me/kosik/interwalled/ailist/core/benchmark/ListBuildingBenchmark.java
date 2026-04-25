package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.AIList;
import me.kosik.interwalled.ailist.core.AIListBuilder;
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

    @Param({ "100000" })
    public int databaseRowsCount;

    protected final Map<String, ArrayList<Interval>> dataSources = new HashMap<>();

    @Setup
    public void setup() {
        BenchmarkData.initializeDatabaseSources(databaseRowsCount, dataSources);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(1)
    @Warmup(        iterations =  3, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(   iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    public void aiListBuildBenchmark(final Blackhole blackhole) {
        ArrayList<Interval> inputData = (ArrayList<Interval>) dataSources.get(dataSource).clone();

        AIListBuilder aiListBuilder = new AIListBuilder(Configuration.DEFAULT);
        List<AIList> aiLists = aiListBuilder.build(inputData);

        for(AIList aiList: aiLists)
            blackhole.consume(aiList);
    }

    public static void main(String[] args) throws Exception {
        String fullClassName = ListBuildingBenchmark.class.getName();
        org.openjdk.jmh.Main.main(new String[] { fullClassName + ".aiListBuildBenchmark" });
    }
}
