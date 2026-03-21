package me.kosik.interwalled.ailist.model;

import java.util.Objects;

public record IntervalsPair<T, U>(
    String key,

    long lhsFrom,
    long lhsTo,
    T lhsValue,

    long rhsFrom,
    long rhsTo,
    U rhsValue
) {

    public static <T, U> IntervalsPair<T, U> fromIntervals(final Interval<T> lhs, final Interval<U> rhs) {
        assert lhs != null;
        assert rhs != null;
        assert Objects.equals(lhs.key(), rhs.key());

        return new IntervalsPair<>(
            lhs.key(),
            lhs.from(), lhs.to(), lhs.value(),
            rhs.from(), rhs.to(), rhs.value()
        );
    }
}



