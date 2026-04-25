package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.AIList;
import me.kosik.interwalled.ailist.core.AIListBuilder;
import me.kosik.interwalled.ailist.core.AIListIterator;
import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.model.IntervalsPair;

import java.util.ArrayList;
import java.util.List;


public class ListBuilder {

    public static List<AIList> buildAILists(final List<Interval> list) {
        return buildAILists(Configuration.DEFAULT, list);
    }

    public static List<AIList> buildAILists(final Configuration configuration, final List<Interval> list) {
        final AIListBuilder aiListBuilder = new AIListBuilder(configuration);
        return aiListBuilder.build(new ArrayList<>(list));
    }

    public static List<IntervalsPair> buildActual(final List<AIList> aiLists, final List<Interval> rhs) {
        final List<IntervalsPair> result = new ArrayList<>();

        for(final AIList aiList : aiLists)
            for (final Interval rhsInterval : rhs) {
                AIListIterator resultsIterator = aiList.overlapping(rhsInterval);

                while (resultsIterator.hasNext()) {
                    Interval lhsInterval = resultsIterator.next();
                    result.add(new IntervalsPair(lhsInterval, rhsInterval));
                }
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

