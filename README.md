## Description

The goal of this tool is to help Spark users find common errors in their batch jobs by analyzing their event logs.

### Supported checks

Currently, the following checks are supported:

1. Multiple actions performed on a dataframe without caching.
2. The same action performed on a dataframe without caching.
3. Task durations within a single stage are too skewed.
4. Spills occurred during stage execution.

### Limitations

1. Does not currently support analyzing jobs that failed.
2. Only intended for large batch jobs, not for streaming.
3. Does not analyze in real time.
4. Very slow on large logs.

### Alternatives

I couldn't find any free and open source alternatives that are still actively supported. There are multiple paid services that likely do a much better job, but they usually require a user to send Spark event logs to their servers, which is a dealbreaker for many.

## Usage

Build and run using sbt. The first command line argument is the path to a Spark event log file.

See Spark [documentation page](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact) for how to record and store event logs.
See [Scala documentation](https://docs.scala-lang.org/getting-started/sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html#running-the-project) for how to run your project with sbt.

## Example output

```
[Performance] Multiple actions without cache: SQL query with id 1 shares a sequence of operation with SQL query with id 0.
  First shared Spark plan node: 'Range': Range (0, 5, step=1, splits=5).
  Last shared Spark plan node: 'MapElements': MapElements org.spark_heuristics.analyzer.heuristics.MultipleActionsWithoutCacheTest$$Lambda$1481/0x000001de6cac3398@558575fe, obj#4: java.lang.Long.
  Consider caching after the last shared node to improve performance.
```
