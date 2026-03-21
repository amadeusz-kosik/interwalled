package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.Intervals;

import java.io.Serializable;


public class AIList<T> implements Serializable {

    // All intervals
    private final Intervals<T> intervals;

    // Number of components (sub lists of intervals).
    private final int componentsCount;

    // Mapping of component index to component's length.
    private final int[] componentsLengths;

    // Mapping of component index to component's starting index (offset) in {intervals}.
    private final int[] componentsStartIndexes;

    // Mapping of component index to maximum 'end' value in of all component's intervals.
    private final long[] componentsMaxEnds;

    AIList(
            final Intervals<T> intervals,
            final int       componentsCount,
            final int[]     componentsLengths,
            final int[]     componentsStartIndexes,
            final long[]    componentsMaxEnds)
    {
        this.intervals              = intervals;
        this.componentsCount        = componentsCount;
        this.componentsLengths      = componentsLengths;
        this.componentsStartIndexes = componentsStartIndexes;
        this.componentsMaxEnds      = componentsMaxEnds;
    }

    public <U> AIListIterator<T> overlapping(Interval<U> interval) {
        return new AIListIterator<>(interval.from(), interval.to(), this);
    }

    /* OverlapIterator interface. */

    public int size() {
        return intervals.length();
    }

    int getComponentStartIndex(final int componentIndex) {
        return componentsStartIndexes[componentIndex];
    }

    int getComponentLength(final int componentIndex) {
        return componentsLengths[componentIndex];
    }

    long getComponentMaxEnd(final int componentIndex) {
        int componentStartIndex = getComponentStartIndex(componentIndex);
        int componentLength     = getComponentLength(componentIndex);
        int componentEndIndex   = componentStartIndex + componentLength - 1;

        return componentsMaxEnds[componentEndIndex];
    }

    long getIntervalMaxEnd(final int intervalIndex) {
        return componentsMaxEnds[intervalIndex];
    }

    int getComponentsCount() {
        return componentsCount;
    }

    Interval<T> getInterval(final int index) {
        return intervals.get(index);
    }

    Intervals<T> getIntervals() {
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

            for(int intervalIndex = 0; intervalIndex < componentsLengths[componentIndex]; ++ intervalIndex) {
                int intervalRealIndex = componentsStartIndexes[componentIndex] + intervalIndex;
                Interval<T> interval = intervals.get(intervalRealIndex);

                sb.append("<");
                sb.append(interval.from());
                sb.append("- ");
                sb.append(interval.to());
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
