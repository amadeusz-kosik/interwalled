package me.kosik.interwalled.ailist.core.model;

public record Interval(
    long id,
    long from,
    long to
) {

    public static boolean overlaps(final Interval lhs, final Interval rhs) {
        return lhs.from <= rhs.to && rhs.from <= lhs.to;
    }
}
