# MiniSpark

A multithreaded data processing framework written in C, inspired by Apache Spark's RDD (Resilient Distributed Dataset) model. Supports lazy evaluation, DAG-based task scheduling, and parallel execution via a custom thread pool.

## Features

- **RDD abstraction** with partitioned data and lazy transformations
- **Transformations:** `map`, `filter`, `join`, `partitionBy`
- **Actions:** `count`, `print` — trigger execution of the DAG
- **Thread pool** with automatic worker count based on available CPU cores
- **DAG traversal** — recursively schedules tasks from leaves to root, respecting dependencies
- **Task metrics** — logs creation time, scheduling time, and execution duration to `metrics.log`
- **File-backed RDDs** — read input data from files, one file per partition

## Usage

### Compile

```bash
gcc -o minispark minispark.c -lpthread
```

### API Overview

```c
// Initialize the framework
MS_Run();

// Build an RDD pipeline
RDD *input = RDDFromFiles(filenames, numfiles);
RDD *mapped = map(input, my_mapper);
RDD *filtered = filter(mapped, my_filter, ctx);

// Trigger execution
int n = count(filtered);
print(filtered, my_printer);

// Clean up
MS_TearDown();
```

## Architecture

The framework follows a Spark-like execution model:

1. **Build** — construct a DAG of RDD transformations (lazy, no computation yet)
2. **Execute** — an action (`count`/`print`) triggers a traversal from the target RDD down to the file-backed leaves
3. **Schedule** — leaf tasks are submitted to the thread pool first; as they complete, dependent tasks are unlocked and submitted
4. **Collect** — the action blocks until all tasks finish, then reads results from the final RDD's partitions

### Thread Pool

Workers are created equal to `(CPU cores - 1)`. Each worker pulls tasks from a shared queue, executes them, and signals dependent RDDs when finished. A separate metrics thread asynchronously logs task timing data.

### Transformations

| Transform | Description |
|---|---|
| `map` | Apply a function to each element, producing a new element |
| `filter` | Keep elements that pass a predicate |
| `join` | Cartesian join across two RDDs' matching partitions |
| `partitionBy` | Repartition data across a new number of partitions |

## Project Structure

| File | Description |
|---|---|
| `minispark.c` | Implementation — thread pool, task execution, RDD operations |
| `minispark.h` | Header — RDD struct, function signatures, type definitions |

## Metrics

Task metrics are written to `metrics.log` with the following fields per task: RDD pointer, partition number, transform type, creation timestamp, scheduled timestamp, and execution duration in microseconds.
