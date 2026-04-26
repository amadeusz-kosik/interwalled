package me.kosik.interwalled.ailist.core.utils;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.ArrayList;


public final class SearchUtils {

    public static int findRightmost(ArrayList<Interval> intervals, long queryStart) {
        return findRightmost(intervals, queryStart, 63);
    }

    public static int findRightmost(ArrayList<Interval> intervals, long queryEnd, long binaryCutoff) {
        // EDGE CASE:
        // All elements are less than queryEnd
        if (intervals.get(intervals.size() - 1).to() < queryEnd)
            return intervals.size() - 1;

        // EDGE CASE:
        // All elements are greater than queryEnd
        if (intervals.get(0).from() > queryEnd) {
            return -1;
        }

        int leftBound = 0;
        int rightBound = intervals.size() - 1;

        while(rightBound -  leftBound > binaryCutoff) {
            int middleIndex = (leftBound + rightBound) / 2;

            if (intervals.get(middleIndex).from() >= queryEnd)
                rightBound = middleIndex;
            else
                leftBound = middleIndex;
        }

        for (int index = rightBound; index >= leftBound; index--) {
            if (intervals.get(index).from() <= queryEnd) {
                return index;
            }
        }

        return -1;
    }
}