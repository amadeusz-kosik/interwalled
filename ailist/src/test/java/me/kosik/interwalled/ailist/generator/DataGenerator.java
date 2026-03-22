package me.kosik.interwalled.ailist.generator;

import me.kosik.interwalled.ailist.model.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class DataGenerator {

    public static List<Interval> generateIntervals(
            final int rowsCount,
            final Function <Integer, Interval> intervalFn
    ) {
        List<Interval> result = new ArrayList<>(rowsCount);

        for(int i = 0; i < rowsCount; i++) {
            result.add(intervalFn.apply(i));
        }

        return result;
    }
}
