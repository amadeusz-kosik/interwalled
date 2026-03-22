package me.kosik.interwalled.ailist.utils;


import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.Intervals;

import java.util.ArrayList;


public class LinearSearch {

    public static int findRightmost(
            final Intervals intervals,
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