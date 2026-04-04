package me.kosik.interwalled.ailist.model;

import java.io.Serializable;
import java.util.ArrayList;

public class Intervals implements Serializable {

    final public Interval[] intervals;

    public Intervals(final Interval[] intervals) {
        this.intervals = intervals;
    }

    public Intervals(final ArrayList<Interval> intervals) {
        this.intervals = intervals.toArray((Interval []) new Interval[0]);
    }

    public Interval get(int index) {
        return intervals[index];
    }

    public int length() {
        return intervals.length;
    }
}