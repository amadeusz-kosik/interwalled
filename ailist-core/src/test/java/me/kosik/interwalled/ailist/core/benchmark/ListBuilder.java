package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.*;
import me.kosik.interwalled.ailist.core.model.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class ListBuilder {

    public static List<AIList> buildAILists(final List<Interval> list) {
        return buildAILists(Configuration.apply(), list);
    }

    public static List<AIList> buildAILists(final Configuration configuration, final List<Interval> list) {
        final AIListBuilder aiListBuilder = new AIListBuilder(configuration);
        return aiListBuilder.build(new ArrayList<>(list));
    }

    public static List<IntervalsPair> buildActual(final List<AIList> aiLists, final List<Interval> rhs) {
        final List<IntervalsPair> result = new ArrayList<>();

        for(final AIList aiList : aiLists)
            for (final Interval rhsInterval : rhs) {
                List<Interval> partialResult = aiList.overlappingJ(rhsInterval);

                for(Interval lhsInterval: partialResult) 
                    result.add(new IntervalsPair(lhsInterval, rhsInterval));                
            }



        result.sort(new IntervalsPairComparator());
        return result;
    }

    public static  List<IntervalsPair> buildExpected(final List<Interval> lhs, final List<Interval> rhs) {
        final List<IntervalsPair> result = new ArrayList<>();

        for(final Interval lhsInterval : lhs) { for (final Interval rhsInterval : rhs) {
            if (Interval.overlaps(lhsInterval, rhsInterval)) {
                result.add(new IntervalsPair(lhsInterval, rhsInterval));
            }
        }}

        result.sort(new IntervalsPairComparator());
        return result;
    }
}

class IntervalsPairComparator implements Comparator<IntervalsPair> {
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
