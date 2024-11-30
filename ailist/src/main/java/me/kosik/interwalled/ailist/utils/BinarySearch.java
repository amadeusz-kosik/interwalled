package me.kosik.interwalled.ailist.utils;

import java.util.ArrayList;
import me.kosik.interwalled.domain.Interval;


public class BinarySearch {

    public static <T> int findRightmost(
            final ArrayList<Interval<T>> intervals,
            final int leftBound,
            final int rightBound,
            final long queryEnd
    ) {
        // EDGE CASE:
        // All elements are less than the {queryEnd}:
        if(intervals.get(rightBound).to() < queryEnd) {
            return rightBound;
        }

        // EDGE CASE:
        // All elements are greater than the {queryEnd}:
        if(intervals.get(leftBound).from() > queryEnd) {
            return -1;
        }

        int leftIndex = leftBound;
        int rightIndex = rightBound;

        // Binary search:
        while(rightIndex - leftIndex > 15) {
            int middleIndex = (leftIndex + rightIndex) / 2;

            if(intervals.get(middleIndex).from() >= queryEnd) {
                // Intervals' left edge is further right than the query's right edge.
                //  The middleIndex is too far right (will not find anything there) and the right side of the array
                //  does not contain any valid queries, skip.
                rightIndex = middleIndex;
            } else {
                leftIndex = middleIndex;
            }
        }

        return LinearSearch.findRightmost(intervals, leftIndex, rightIndex, queryEnd);
    }
}
