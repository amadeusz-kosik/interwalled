package me.kosik.interwalled.ailist.model;

import java.io.Serializable;
import java.util.ArrayList;

public class Intervals<T> implements Serializable {

    final private Interval<T>[] intervals;

    @SuppressWarnings("unchecked")
    public Intervals(final ArrayList<Interval<T>> intervals) {
        this.intervals = intervals.toArray((Interval<T> []) new Interval[0]);
    }

    public Interval<T> get(int index) {
        return intervals[index];
    }

    public int length() {
        return intervals.length;
    }
}