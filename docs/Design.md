# Task runtime

## 1. Purpose

The multicast is built on the course message platform (`QueueBroker`, `MessageQueue`, tasks, and circular buffers). Every task runs in the same Java runtime. This design extends that platform with a Lamport logical clock on each task and with fail-stop termination.

The behaviour specified here is implemented by `edu.polytech.queues.Task`, `edu.polytech.queues.TaskKilled`, `edu.polytech.utils.CTask`, and `edu.polytech.utils.Executor`.

## 2. Project layout

The Eclipse project is the directory `Salma_KAMAL`. Its name is `SalmaKAMAL`. Import it with **File → Import → Existing Projects into Workspace**. The project carries its compiler settings and needs no extra library.

| Source folder | Role |
|---|---|
| `api` | `Task`, `QueueBroker`, `MessageQueue` |
| `local` | Local queues and brokers |
| `utils.queues` | `CTask`, `Executor`, `CircularBuffer` |
| `tests` | `TestLogicalClock` |

The compiler compliance is Java 17. The output directory is `bin`.

## 3. Logical clock

Each task holds one clock, a non-negative integer, initially `0`.

The clock ticks once when a runnable posted on that task begins execution, before the body of the runnable runs. The value observed inside the runnable is therefore the value after the tick. A runnable that is never started does not tick the clock.

`witness(timestamp)` records a timestamp observed from outside the task:

```
clock = max(clock, timestamp)
```

A negative or smaller timestamp leaves the clock unchanged. The next runnable then ticks from that value, so local events stay strictly increasing.

`logicalClock()` returns the current value. It does not tick.

## 4. Fail-stop

A task is either alive or dead. `dead()` is true after `kill()`, `fail`, or `exit`.

`kill()` is the explicit failure. It marks the task dead and discards every runnable still waiting on it, including delayed runnables. Those runnables never execute. The same destruction applies to every task created from it by `newTask`. A `post` issued after the task is dead is ignored. The runnable that already started is not aborted in the middle of its body.

Random failure uses the same destruction. Each task has a boolean `mayFail`, false by default. Before a runnable starts, the runtime draws a failure only if `mayFail` is true. The probability is set by `setFailureProbability` and belongs to the closed interval `[0, 1]`. A probability of `0` never kills a task. A probability of `1` kills a failable task instead of starting its next runnable, and that runnable does not tick the clock. A task with `mayFail` false is never selected.

On death, every message queue registered on the task is closed. If a listener was registered with `set`, it is notified through `failed`. A `TaskKilled` cause is the crash signal produced by `kill()` and by a random failure.

## 5. Operations

| Operation | Effect |
|---|---|
| `logicalClock()` | Current clock, without ticking |
| `witness(long)` | `clock = max(clock, timestamp)` |
| `kill()` | Dead task, waiting runnables dropped, created tasks killed |
| `setMayFail(boolean)` | Enables or disables random failure for this task |
| `mayFail()` | Whether random failure applies |
| `setFailureProbability(double)` | Probability in `[0, 1]` for the next runnable of a failable task |
| `post(Runnable)` | Ignored when the task is dead |
| `Task.task()` | The task whose runnable is executing |

## 6. Test

`edu.polytech.queues.tests.TestLogicalClock` checks the clock, `witness`, `kill` (including tasks created from the killed task), and a random failure that strikes only a failable task.

From the directory `Salma_KAMAL`:

```bash
javac -d bin $(find api local utils.queues tests -name '*.java')
java -cp bin edu.polytech.queues.tests.TestLogicalClock
```

The successful run prints `logical clock tests: PASS`.
