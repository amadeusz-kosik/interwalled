package me.kosik.interwalled.ailist.utils;

import me.kosik.interwalled.ailist.model.IntervalsPair;

import java.util.Comparator;

public class IntervalsPairComparator<T, U> implements Comparator<IntervalsPair<T, U>> {
    @Override
    public int compare(IntervalsPair<T, U> lhs, IntervalsPair<T, U> rhs) {
        if (lhs.lhsFrom() > rhs.lhsFrom()) return  1;
        if (lhs.lhsFrom() < rhs.lhsFrom()) return -1;

        if (lhs.rhsFrom() > rhs.rhsFrom()) return  1;
        if (lhs.rhsFrom() < rhs.rhsFrom()) return -1;

        if (lhs.lhsTo() > rhs.lhsTo()) return  1;
        if (lhs.lhsTo() < rhs.lhsTo()) return -1;

        if (lhs.rhsTo() > rhs.rhsTo()) return  1;
        if (lhs.rhsTo() < rhs.rhsTo()) return -1;

        return 0;
    }
}