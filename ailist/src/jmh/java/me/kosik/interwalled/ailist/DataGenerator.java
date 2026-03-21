package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Interval;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.util.ArrayList;


public class DataGenerator {

    final public static String DEFAULT_KEY = "CH1";

    public static ArrayList<Interval<String>> consecutive(final int rowsCount) {
        return intervals(rowsCount, 2, 0, new ArrayList<>(rowsCount));
    }

    public static ArrayList<Interval<String>> overlapping(final int rowsCount) {
        return intervals(rowsCount, 2, 1, new ArrayList<>(rowsCount));
    }

    public static ArrayList<Interval<String>> wide(final int rowsCount) {
        return intervals(rowsCount, 256, 0, new ArrayList<>(rowsCount));
    }

    public static ArrayList<Interval<String>> lasting(final int rowsCount) {
        return intervals(rowsCount, 16, 16, new ArrayList<>(rowsCount));
    }

    public static ArrayList<Interval<String>> mixed(final int rowsCount, final int layers) {
        assert layers > 0;

        ArrayList<Interval<String>> output = new ArrayList<>(rowsCount * layers);

        for (int i = 1; i <= layers; i++) {
            intervals(rowsCount, 4 * i, 0, output);
        }

        return output;
    }

    public static ArrayList<Interval<String>> querySparse(final int databaseRowsCount, final int queryRowsCount) {
        ArrayList<Interval<String>> output = new ArrayList<>(queryRowsCount);
        return intervals(queryRowsCount, databaseRowsCount / queryRowsCount, 0, output);
    }

    public static ArrayList<Interval<String>> queryDense(final int databaseRowsCount, final int queryRowsCount) {
        ArrayList<Interval<String>> output = new ArrayList<>(queryRowsCount);
        return intervals(queryRowsCount, databaseRowsCount / queryRowsCount, 512, output);
    }

    private static ArrayList<Interval<String>> intervals(final int rowsCount, final int width, final int overlap, final ArrayList<Interval<String>> output) {
        assert rowsCount >  0;
        assert width     >= 0;
        assert overlap   >= 0;

        for(int i = 0; i < rowsCount; i++) {
            final long from = (long) i * width - (overlap / 2);
            final long to   = (long) from + width - 1 + overlap;

            output.add(new Interval<>(DEFAULT_KEY, from, to, String.valueOf(i)));
        }

        return output;
    }

    public static ArrayList<Interval<String>> shortPoisson(final int rowsCount) {
        assert rowsCount >  0;

        final int WIDTH_MEAN = 16;

        ArrayList<Interval<String>> result = new ArrayList<>(rowsCount);
        RandomGenerator randomGenerator    = new Well19937c(1337);
        PoissonDistribution generator      = new PoissonDistribution(
                randomGenerator,
                PoissonDistribution.DEFAULT_EPSILON,
                PoissonDistribution.DEFAULT_MAX_ITERATIONS,
                WIDTH_MEAN
        );

        for(int i = 0; i < rowsCount; i++) {
            final int width = generator.sample();
            final long from = (long) i * (WIDTH_MEAN + 1);
            final long to = from + width;

            result.add(new Interval<>(DEFAULT_KEY, from, to, String.valueOf(i)));
        }

        return result;
    }
}
