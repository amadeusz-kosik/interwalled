package me.kosik.interwalled.ailist.core.utils;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.Comparator;

public class IntervalComparator implements Comparator<Interval> {
    @Override
    public int compare(final Interval lhs, final Interval rhs) {
        if (lhs.from() > rhs.from()) return  1;
        if (lhs.from() < rhs.from()) return -1;

        if (lhs.to() > rhs.to()) return  1;
        if (lhs.to() < rhs.to()) return -1;

        return 0;
    }
}
