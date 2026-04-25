package me.kosik.interwalled.ailist.core.generator;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.List;
import java.util.function.Function;

public class DataGenerators {

    public static List<Interval> intervals(final int rowsCount, final Function<Integer, Interval> intervalFn) {
        return DataGenerator.generateIntervals(rowsCount, intervalFn);
    }

    public static List<Interval> consecutiveIntervals(final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i, i, i));
    }

    public static List<Interval> consecutiveIntervals(final int offset, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i, i + offset, i + offset));
    }

    public static List<Interval> consecutiveIntervals(final int offset, final int rowWidth, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i, i + offset, i + offset + rowWidth));
    }
}
