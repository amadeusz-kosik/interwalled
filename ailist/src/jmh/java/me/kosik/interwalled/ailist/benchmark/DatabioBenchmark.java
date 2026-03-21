package me.kosik.interwalled.ailist.benchmark;

import me.kosik.interwalled.ailist.AIList;
import me.kosik.interwalled.ailist.AIListBuilder;
import me.kosik.interwalled.ailist.AIListIterator;
import me.kosik.interwalled.ailist.DataSources;
import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class DatabioBenchmark {

    @Param({
        "databio-s-1-2",
        "databio-s-2-7",
        "databio-s-1-0"
    })
    public String dataSource;

    private final Map<Integer, ArrayList<Interval<String>>> datasets = new HashMap<>();

    @Setup
    public void setup() {
        try {
            datasets.put(0, loadDataset("chainRn4"));
            datasets.put(1, loadDataset("fBrain-DS14718"));
            datasets.put(2, loadDataset("exons"));
            datasets.put(7, loadDataset("ex-anno"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load datasets.", e);
        }
    }

    private ArrayList<Interval<String>> loadDataset(final String name) throws IOException {
        final String datasetFilePath = "data/databio-csv/" + name + ".csv";
        final ArrayList<Interval<String>> intervals = new ArrayList<>();
        final BufferedReader reader = new BufferedReader(new FileReader(datasetFilePath));

        // Skip header
        reader.readLine();

        for(;;) {
            String line = reader.readLine();

            if (line == null) { break; }
            final String[] fields = line.split(",");

            intervals.add(new Interval<>(
                fields[0],
                Integer.parseInt(fields[1]),
                Integer.parseInt(fields[2]),
                UUID.randomUUID().toString()
            ));
        }

        return intervals;
    }

    @Benchmark
    public void benchmarkJoin(final Blackhole blackhole) {
        final int databaseId = Integer.parseInt(dataSource.split("-")[2]);
        final int queryId    = Integer.parseInt(dataSource.split("-")[3]);

        final AIListBuilder<String> aiListBuilder =
            new AIListBuilder<>(AIListConfiguration.DEFAULT, datasets.get(databaseId));

        final AIList<String> aiList = aiListBuilder.build();

        for(Interval<String> interval: datasets.get(queryId)) {
            for (AIListIterator<String> it = aiList.overlapping(interval); it.hasNext(); ) {
                Interval<String> dbInterval = it.next();
                blackhole.consume(dbInterval);
            }
        }
    }

}
