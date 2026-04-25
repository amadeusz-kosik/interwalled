package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.model.IntervalsPair;
import java.util.Comparator;


public class IntervalsPairComparator implements Comparator<IntervalsPair> {
    @Override
    public int compare(IntervalsPair lhs, IntervalsPair rhs) {
        if (lhs.left().from() > rhs.left().from()) return  1;
        if (lhs.left().from() < rhs.left().from()) return -1;

        if (lhs.right().from() > rhs.right().from()) return  1;
        if (lhs.right().from() < rhs.right().from()) return -1;

        if (lhs.left().to() > rhs.left().to()) return  1;
        if (lhs.left().to() < rhs.left().to()) return -1;

        if (lhs.right().to() > rhs.right().to()) return  1;
        if (lhs.right().to() < rhs.right().to()) return -1;

        return 0;
    }
}