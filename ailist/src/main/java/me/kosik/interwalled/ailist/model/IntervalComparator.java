package me.kosik.interwalled.ailist.model;

import java.io.Serializable;
import java.util.Comparator;

public class IntervalComparator {
    public static <T> Comparator<Interval> comparing() {
        return (Comparator<Interval> & Serializable) (Interval lhs, Interval rhs) ->
                (lhs.from() == rhs.from()) ? Long.compare(lhs.to(), rhs.to()) : Long.compare(lhs.from(), rhs.from());
    }
}
