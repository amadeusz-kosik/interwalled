package me.kosik.interwalled.algorithm.ailist;

import me.kosik.interwalled.algorithm.Interval;
import me.kosik.interwalled.algorithm.IntervalHolder;

import java.util.ArrayList;
import java.util.Iterator;

// Augmented Interval List implementation
public class AIList<T> implements IntervalHolder<T> {

    // All intervals
    private final ArrayList<Interval<T>> intervals;

    // Number of components (sub lists of intervals).
    private final int componentsCount;

    // Mapping of component index to component's length.
    private final ArrayList<Integer> componentsLengths;

    // Mapping of component index to component's starting index (offset) in {intervals}.
    private final ArrayList<Integer> componentsStartIndexes;

    // Mapping of component index to maximum 'end' value in of all component's intervals.
    private final ArrayList<Long> componentsMaxEnds;

    AIList(
            final ArrayList<Interval<T>> intervals,
            final int componentsCount,
            final ArrayList<Integer> componentsLengths,
            final ArrayList<Integer> componentsStartIndexes,
            final ArrayList<Long> componentsMaxEnds)
    {
        this.intervals = intervals;
        this.componentsCount = componentsCount;
        this.componentsLengths = componentsLengths;
        this.componentsStartIndexes = componentsStartIndexes;
        this.componentsMaxEnds = componentsMaxEnds;
    }

    @Override
    public Iterator<Interval<T>> overlapping(Interval<T> interval) {
        return new OverlapIterator<>(interval.start(), interval.end(), this);
    }

    /* OverlapIterator interface. */

    int getComponentStartIndex(final int componentIndex) {
        return componentsStartIndexes.get(componentIndex);
    }

    int getComponentLength(final int componentIndex) {
        return componentsLengths.get(componentIndex);
    }

    long getComponentMaxEnd(final int componentIndex) {
        return componentsMaxEnds.get(componentIndex);
    }

    int getComponentsCount() {
        return componentsCount;
    }

    Interval<T> getInterval(final int index) {
        return intervals.get(index);
    }
}
