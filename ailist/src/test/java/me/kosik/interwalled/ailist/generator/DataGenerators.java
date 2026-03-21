package me.kosik.interwalled.ailist.generator;

import me.kosik.interwalled.ailist.model.Interval;

import java.util.List;
import java.util.function.Function;

public class DataGenerators {

    public static String DEFAULT_KEY = "CH1";

    public static <T> List<Interval<T>> intervals(final int rowsCount, final Function<Integer, Interval<T>> intervalFn) {
        return DataGenerator.generateIntervals(rowsCount, intervalFn);
    }

    public static <T> List<Interval<String>> consecutiveIntervals(final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval<>(DEFAULT_KEY, i, i, i.toString()));
    }

    public static List<Interval<String>> consecutiveIntervals(final int offset, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval<>(DEFAULT_KEY, i + offset, i + offset, i.toString()));
    }

    public static List<Interval<String>> consecutiveIntervals(final int offset, final int rowWidth, final int rowsCount) {
        return DataGenerator.generateIntervals(rowsCount, i -> new Interval<>(DEFAULT_KEY, i + offset, i + offset + rowWidth, i.toString()));
    }
}
