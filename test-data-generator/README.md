

## Available test cases
One to all
: Uniform intervals (left) and a single interval matching all intervals (right).

One to one
: Uniform intervals (left, right) matching 1:1.

Sparse (`i`)
: Every i-th interval from (right) has matching interval from (left).

Spanning (`i`)
: Every linear interval (left) matches 2 times i intervals (right) - it spans from `(from - i)` to `(to + 1)` instead
of from `from` to `to`. 

Skewed(`i`)
: `i`% is centered around point 0, (1 - i)% is the uniform tail.