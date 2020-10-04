# EPFL Database Systems class - Project2

This repo contains the solution of the Database System class @ EPFL, developed by me and [@manuleo](https://github.com/manuleo).

We used Scala and Spark (using RDDs only for the first 2 exercises, as Spark SQL and Spark DataFrame were not allowed). This project can be divided in 3 tasks:
- [Implementation](src/main/scala/rollup) of the ROLLUP operator.
- [Implementation](src/main/scala/thetajoin) of a MapReduce‑friendly theta‑join, according to the [1‑Bucket‑Theta Algorithm](https://dl.acm.org/doi/10.1145/1989323.1989423) by Okcan et al. with additional Reduce optimizations.
- [Implementation](src/main/scala/lsh) of approximated kNNs via Jaccard-similarity locality sensitive hash functions.

We have also written [unit tests](https://github.com/dedeswim/db-spark-projects/tree/master/src/test/scala) for the latter 2 tasks.

Finally, we have also written a [report](Project2.pdf) about the results we obtained.

Our project has been graded 6/6.
