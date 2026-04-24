package me.kosik.interwalled.ailist.core.model;

public record Interval<T>(
    long from,
    long to,
    T value
) {}
