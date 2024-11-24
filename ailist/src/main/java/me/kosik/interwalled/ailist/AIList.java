package me.kosik.interwalled.ailist;

import java.io.Serializable;
import java.util.ArrayList;

public class AIList<T> implements Serializable {

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

    public OverlapIterator<T> overlapping(Interval<T> interval) {
        return new AIListIterator<>(interval.start(), interval.end(), this);
    }

    /* OverlapIterator interface. */

    int size() {
        return intervals.size();
    }

    int getComponentStartIndex(final int componentIndex) {
        return componentsStartIndexes.get(componentIndex);
    }

    int getComponentLength(final int componentIndex) {
        return componentsLengths.get(componentIndex);
    }

    long getComponentMaxEnd(final int componentIndex) {
        int componentStartIndex = getComponentStartIndex(componentIndex);
        int componentLength = getComponentLength(componentIndex);
        int componentEndIndex = componentStartIndex + componentLength - 1;

        return componentsMaxEnds.get(componentEndIndex);
    }

    long getIntervalMaxEnd(final int intervalIndex) {
        return componentsMaxEnds.get(intervalIndex);
    }

    int getComponentsCount() {
        return componentsCount;
    }

    Interval<T> getInterval(final int index) {
        return intervals.get(index);
    }

    ArrayList<Interval<T>> getIntervals() {
        return intervals;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AIList { Components: ");
        sb.append(componentsCount);
        sb.append("; ");

        for(int componentIndex = 0; componentIndex < componentsCount; componentIndex++) {
            sb.append("Component #");
            sb.append(componentIndex);
            sb.append("{");

            for(int intervalIndex = 0; intervalIndex < componentsLengths.get(componentIndex); ++ intervalIndex) {
                int intervalRealIndex = componentsStartIndexes.get(componentIndex) + intervalIndex;
                Interval<T> interval = intervals.get(intervalRealIndex);

                sb.append("<");
                sb.append(interval.start());
                sb.append("- ");
                sb.append(interval.end());
                sb.append("; ");
                sb.append(getIntervalMaxEnd(intervalRealIndex));
                sb.append(">");
            }

            sb.append("} ");
        }

        sb.append("}");

        return sb.toString();
    }
}
