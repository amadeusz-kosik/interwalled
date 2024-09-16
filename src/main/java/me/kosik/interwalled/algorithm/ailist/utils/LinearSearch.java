package me.kosik.interwalled.algorithm.ailist.utils;

import me.kosik.interwalled.algorithm.Interval;

import java.util.ArrayList;

public class LinearSearch {
    public static <T> int findRightmost(
            final ArrayList<Interval<T>> intervals,
            final int leftBound,
            final int rightBound,
            final long queryEnd
    ) {
        for(int index = rightBound; index >= leftBound; -- index) {
            if(intervals.get(index).start() <= queryEnd)
                return index;
        }

        return -1;
    }
}
