package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.ArrayList;

public class AIList<T> {

    final private ArrayList<Interval<T>> intervals;

    final private long[] maxE;

    public AIList(ArrayList<Interval<T>> intervals) {
        this.intervals = intervals;
        this.maxE = new long[intervals.size()];

        long lastMaxE = intervals.get(0).to();

        for(int i = 0; i < intervals.size(); ++ i) {
            lastMaxE = Math.max(lastMaxE, intervals.get(i).to());
            this.maxE[i] = lastMaxE;
        }
    }
}
