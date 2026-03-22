package me.kosik.interwalled.ailist.utils;

import me.kosik.interwalled.ailist.model.IntervalsPair;

import java.util.Comparator;

public class IntervalsPairComparator implements Comparator<IntervalsPair> {
    @Override
    public int compare(IntervalsPair lhs, IntervalsPair rhs) {
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