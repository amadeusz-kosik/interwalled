package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.ArrayList;
import java.util.Arrays;

public class AIList {

    final ArrayList<Interval> intervals;

    final long[] maxE;

    public AIList(ArrayList<Interval> intervals) {
        this.intervals = intervals;
        this.maxE = new long[intervals.size()];

        if(! intervals.isEmpty()) {
            long lastMaxE = intervals.get(0).to();

            for(int i = 0; i < intervals.size(); ++ i) {
                lastMaxE = Math.max(lastMaxE, intervals.get(i).to());
                this.maxE[i] = lastMaxE;
            }
        }
    }

    public int size() {
        return intervals.size();
    }

    public AIListIterator overlapping(final Interval interval) {
        return new AIListIterator(interval.from(), interval.to(), this);
    }

    @Override
    public String toString() {
        return "AIList{" +
                "intervals=" + intervals +
                ", maxE=" + Arrays.toString(maxE) +
                '}';
    }
}
