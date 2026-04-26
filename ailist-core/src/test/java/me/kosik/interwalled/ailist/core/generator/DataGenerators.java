package me.kosik.interwalled.ailist.core.generator;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.List;


public class DataGenerators {

    public static List<Interval> consecutiveIntervals(final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i, i));
    }

    public static List<Interval> consecutiveIntervals(final int offset, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i + offset, i + offset));
    }

    public static List<Interval> consecutiveIntervals(final int offset, final int rowWidth, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval(i + offset, i + offset + rowWidth));
    }
}
