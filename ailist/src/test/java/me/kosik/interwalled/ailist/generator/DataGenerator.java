package me.kosik.interwalled.ailist.generator;

import me.kosik.interwalled.ailist.model.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class DataGenerator {

    public static <T> List<Interval<T>> generateIntervals(
            final int rowsCount,
            final Function <Integer, Interval<T>> intervalFn
    ) {
        List<Interval<T>> result = new ArrayList<>(rowsCount);

        for(int i = 0; i < rowsCount; i++) {
            result.add(intervalFn.apply(i));
        }

        return result;
    }
}
