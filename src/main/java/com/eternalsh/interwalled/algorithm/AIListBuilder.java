package com.eternalsh.interwalled.algorithm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

public class AIListBuilder<T> implements IntervalHolderBuilder<T, AIList<T>>, Serializable {

    private final int maximumComponentsCount;
    private final int intervalsCountToCheckLookahead;
    private final int intervalsCountToTriggerExtraction;
    private final int minimumComponentSize;

    private ArrayList<Interval<T>> intervals = new ArrayList<>();

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

    @Override
    public AIList<T> build() {
        intervals.sort(Comparator.comparingLong(Interval::start));

        int componentsCount = 0;
        ArrayList<Integer> componentsLengths = new ArrayList<>();
        ArrayList<Integer> componentsStartIndexes = new ArrayList<>();
        ArrayList<Long> componentsMaxEnds = new ArrayList<>();

        if (intervals.size() <= minimumComponentSize) {
            componentsCount = 1;
            componentsLengths.add(intervals.size());
            componentsStartIndexes.add(0);
        } else {
            ArrayList<Interval<T>> decomposed = new ArrayList<>();
            final int inputSize = intervals.size();

            // decompose while
            // max component number is not exceeded
            // and number of intervals left is big enough
            // to be worth decomposing it
            while (componentsCount < maximumComponentsCount && inputSize - decomposed.size() > minimumComponentSize) {
                ArrayList<Interval<T>> remainingIntervals = new ArrayList<>();
                ArrayList<Interval<T>> extractedIntervals = new ArrayList<>();

                for (int i = 0; i < intervals.size(); i++) {
                    final Interval<T> interval = intervals.get(i);

                    int j = 1;
                    int cov = 0;
                    // count intervals covered by i'th interval
                    while (j <= intervalsCountToCheckLookahead && cov < intervalsCountToTriggerExtraction && i + j < intervals.size()) {
                        if (intervals.get(i + j).end() <= interval.end()) {
                            cov++;
                        }
                        j++;
                    }
                    // check if it is worth to extract i'th interval
                    // and if it is do so
                    if (cov < intervalsCountToTriggerExtraction)
                        remainingIntervals.add(interval);
                    else
                        extractedIntervals.add(interval);

                }
                // add the component info
                componentsStartIndexes.add(decomposed.size());
                componentsLengths.add(remainingIntervals.size());
                componentsCount ++;

                // check if list2 will be the last component
                if (extractedIntervals.size() <= minimumComponentSize || componentsCount == maximumComponentsCount - 2) {
                    // no more decomposing, add list2 if not empty
                    // if it is empty, then list1 was already added
                    // in the previous loop
                    if (!extractedIntervals.isEmpty()) {
                        decomposed.addAll(remainingIntervals);
                        componentsStartIndexes.add(decomposed.size());
                        componentsLengths.add(extractedIntervals.size());
                        decomposed.addAll(extractedIntervals);
                        componentsCount ++;
                    } else if (remainingIntervals.size() > minimumComponentSize) {
                        // if last component has size > minimumComponentSize
                        // but can't be decomposed
                        decomposed.addAll(remainingIntervals);
                    }
                } else {
                    // prepare for the next loop
                    decomposed.addAll(remainingIntervals);
                    intervals = extractedIntervals;
                }
            }

            intervals = decomposed;
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

    @Override
    public void put(final Interval<T> interval) {
        intervals.add(interval);
    }
}
