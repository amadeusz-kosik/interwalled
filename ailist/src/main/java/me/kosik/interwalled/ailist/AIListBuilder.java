package me.kosik.interwalled.ailist;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

public class AIListBuilder<T> implements Serializable {

    private final int maximumComponentsCount;
    private final int intervalsCountToCheckLookahead;
    private final int intervalsCountToTriggerExtraction;
    private final int minimumComponentSize;

    private final ArrayList<Interval<T>> intervals = new ArrayList<>();

    public AIListBuilder(
            final int maximumComponentsCount,
            final int intervalsCountToCheckLookahead,
            final int intervalsCountToTriggerExtraction,
            final int minimumComponentSize
    ) {
        this.maximumComponentsCount = maximumComponentsCount;
        this.intervalsCountToCheckLookahead = intervalsCountToCheckLookahead;
        this.intervalsCountToTriggerExtraction = intervalsCountToTriggerExtraction;
        this.minimumComponentSize = minimumComponentSize;
    }

    public AIList<T> build() {
        intervals.sort(Comparator.comparingLong(Interval::start));

        int componentsCount = 0;
        ArrayList<Integer> componentsLengths = new ArrayList<>();
        ArrayList<Integer> componentsStartIndexes = new ArrayList<>();
        ArrayList<Long> componentsMaxEnds = new ArrayList<>();

        if (intervals.size() <= minimumComponentSize || maximumComponentsCount == 1) {
            // Edge case: at start of the algorithm assign everything to a single component.
            componentsCount = 1;
            componentsLengths.add(intervals.size());
            componentsStartIndexes.add(0);
        } else {
            // Decompose lists while:
            //  1/ max component number is not exceeded
            //  2/ number of intervals left is big enough
            //  3/ it is worth to decompose

            int lastAssignedIndex = -1;

            for(int componentIndex = 0; componentIndex < maximumComponentsCount - 1; ++ componentIndex) {
                // If the number of intervals left is smaller than expected minimal component size, then break.
                if(lastAssignedIndex  >= (intervals.size() - minimumComponentSize)) {
                    break;
                }

                int currentComponentStartIndex = lastAssignedIndex + 1;
                int currentComponentLength = 0;

                ArrayList<Interval<T>> extractedIntervals = new ArrayList<>();

                for(int currentIntervalIndex = currentComponentStartIndex; currentIntervalIndex < intervals.size(); ) {
                    final Interval<T> currentInterval = intervals.get(currentIntervalIndex);
                    int coverage = 0;

                    // Count interval's coverage: how many further intervals are "covered" by the current one's length.
                    for(int lookaheadOffset = 1; lookaheadOffset <= intervalsCountToCheckLookahead; ++ lookaheadOffset) {
                        int lookaheadIndex = lookaheadOffset + currentIntervalIndex;

                        // Guard against going outside the intervals' list.
                        //  Break if all intervals are already visited.
                        if (lookaheadIndex >= intervals.size())
                            break;

                        // If current interval is reaching further than the checked
                        //  one, increment coverage
                        if (intervals.get(lookaheadIndex).end() <= currentInterval.end())
                            coverage++;

                        // If enough intervals are already covered, skip browsing the rest.
                        if (coverage >= intervalsCountToTriggerExtraction)
                            break;
                    }

                    if(coverage == intervalsCountToTriggerExtraction) {
                        // Move the current interval to the extracted ones.
                        extractedIntervals.add(currentInterval);
                        intervals.remove(currentIntervalIndex);
                    } else {
                        lastAssignedIndex = currentIntervalIndex;

                        // Move the index right (skip).
                        ++ currentIntervalIndex;
                        ++ currentComponentLength;
                    }
                }

                // Save new component
                ++ componentsCount;
                componentsStartIndexes.add(currentComponentStartIndex); // ERROR
                componentsLengths.add(currentComponentLength);

                // Re-add extracted intervals back to the original list.
                intervals.addAll(extractedIntervals);
            }

            // Put remaining elements to the last component
            if(componentsLengths.stream().mapToLong(Integer::longValue).sum() < intervals.size()) {
                int lastComponentStartIndex = (int) componentsLengths.stream().mapToLong(Integer::longValue).sum();

                ++ componentsCount;

                componentsStartIndexes.add(lastComponentStartIndex);
                componentsLengths.add(intervals.size() - lastComponentStartIndex);
            }
        }

        for (int i = 0; i < componentsCount; i ++) {
            final int componentStart = componentsStartIndexes.get(i);
            final int componentEnd   = componentStart + componentsLengths.get(i);

            long maxEnd = intervals.get(componentStart).end();
            componentsMaxEnds.add(maxEnd);

            for (int j = componentStart + 1; j < componentEnd; j ++) {
                maxEnd = Math.max(intervals.get(j).end(), maxEnd);
                componentsMaxEnds.add(maxEnd);
            }
        }

        return new AIList<>(
            intervals,
            componentsCount,
            componentsLengths,
            componentsStartIndexes,
            componentsMaxEnds
        );
    }

    public void put(final Interval<T> interval) {
        intervals.add(interval);
    }
}
