package me.kosik.interwalled.ailist.utils;

import me.kosik.interwalled.domain.Interval;

import java.util.ArrayList;


public class LinearSearch {
    public static <T> int findRightmost(
            final ArrayList<Interval<T>> intervals,
            final int leftBound,
            final int rightBound,
            final long queryEnd
    ) {
        for(int index = rightBound; index >= leftBound; -- index) {
            if(intervals.get(index).from() <= queryEnd)
                return index;
        }

        return -1;
    }
}
