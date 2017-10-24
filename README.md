# minidb

This is a prototype for a simple in-memory database as described by
[Birrell et al][birrell-et-al].  Build with `cargo build`.  When
running the program, a directory called `/tmp/minidb` will be created
which will contain the data files.  Every time the program is invoked,
this directory is deleted to ensure a clean slate.

[birrell-et-al]: http://birrell.org/andrew/papers/024-DatabasesPaper-SOSP.pdf
