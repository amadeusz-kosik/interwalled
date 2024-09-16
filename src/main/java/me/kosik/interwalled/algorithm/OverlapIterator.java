package me.kosik.interwalled.algorithm;

import java.util.Iterator;

public interface OverlapIterator<T> extends Iterator<Interval<T>>
{
    @Override
    boolean hasNext();

    @Override
    Interval<T> next();
}
