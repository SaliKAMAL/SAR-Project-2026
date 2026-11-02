# Design

The Eclipse project is the directory `Salma_KAMAL`. Its name is `SalmaKAMAL`. Import it with **File → Import → Existing Projects into Workspace**. The project carries its compiler settings and needs no extra library. Compiler compliance is Java 17. The output directory is `bin`.

| Source folder | Role |
|---|---|
| `api` | Public types, including `edu.polytech.multicast` |
| `local` | Local queues and brokers |
| `utils.queues` | `CTask`, `Executor`, `CircularBuffer` |
| `tests` | `TestLogicalClock`, `TestMulticastNoFailure` |

## Soufian El Ouazzani

### Task runtime

The multicast is built on the course message platform (`QueueBroker`, `MessageQueue`, tasks, and circular buffers). Every task runs in the same Java runtime. This part extends that platform with a Lamport logical clock on each task and with fail-stop termination.

The behaviour is implemented by `edu.polytech.queues.Task`, `edu.polytech.queues.TaskKilled`, `edu.polytech.utils.CTask`, and `edu.polytech.utils.Executor`.

Each task holds one clock, a non-negative integer, initially `0`. The clock ticks once when a runnable posted on that task begins execution, before the body of the runnable runs. The value observed inside the runnable is therefore the value after the tick. A runnable that is never started does not tick the clock.

`witness(timestamp)` records a timestamp observed from outside the task:

```
clock = max(clock, timestamp)
```

A smaller timestamp leaves the clock unchanged. `logicalClock()` returns the current value and does not tick.

A task is either alive or dead. `dead()` is true after `kill()`, `fail`, or `exit`. `kill()` marks the task dead and discards every runnable still waiting on it, including delayed runnables. The same destruction applies to every task created from it by `newTask`. A `post` issued after the task is dead is ignored. The runnable that already started is not aborted in the middle of its body.

Random failure uses the same destruction. Each task has a boolean `mayFail`, false by default. Before a runnable starts, the runtime draws a failure only if `mayFail` is true. The probability is set by `setFailureProbability` and belongs to the closed interval `[0, 1]`. A probability of `0` never kills a task. A probability of `1` kills a failable task instead of starting its next runnable, and that runnable does not tick the clock.

On death, every message queue registered on the task is closed. If a listener was registered with `set`, it is notified through `failed`. A `TaskKilled` cause is the crash signal produced by `kill()` and by a random failure.

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

`edu.polytech.queues.tests.TestLogicalClock` checks the clock, `witness`, `kill`, and a random failure that strikes only a failable task. From `Salma_KAMAL`:

```bash
javac -d bin $(find api local utils.queues tests -name '*.java')
java -cp bin edu.polytech.queues.tests.TestLogicalClock
```

The successful run prints `logical clock tests: PASS`.

### Totally-ordered multicast without failures

Every actor stays alive. There is no crash, no join, no leave, and no reinsertion. `generationId` is always `1`.

`GroupManager.start(n, period)` creates `n` actors, gives each one private task, and connects every pair with a `MessageQueue`. It hands them the verifier and is not used after that. `period` is the delay between multicasts, and `0` is burst mode. Each actor then sends `GroupManager.MESSAGES_PER_ACTOR` messages.

An actor sends and receives only through its queues. The runnable that sends has already ticked, so the message is stamped with `logicalClock()`. On reception the actor calls `witness` before storing the message. The sender also keeps its own message and delivers it through the same rule as every other member.

A message carries `messageId`, `senderId`, `generationId`, `lamportTimestamp`, and `payload`. The order is the timestamp, smallest first. Equal timestamps are broken by `senderId`, with the same string order on every actor.

The head message is delivered only when every member of the view has sent a message or an acknowledgement whose timestamp is at least that high. Acknowledgements are not delivered to the application. `Group` holds the sorted member ids and does not remove anyone in this part.

`Verifier` is a plain object, not an actor. On delivery the actor calls `delivered(actorId, messageId, payload)` directly. `check()` fails when two actors differ in the ids, the order, or the payload. `GroupListener` declares `joined`, `left`, `crashed`, `resurrected`, and `reinserted`. This part does not call it.

`edu.polytech.multicast.tests.TestMulticastNoFailure` runs three actors with period `200`, then three actors with period `0`. From `Salma_KAMAL`:

```bash
java -cp bin edu.polytech.multicast.tests.TestMulticastNoFailure
```

The successful run prints `multicast no failure tests: PASS`.

## Salma KAMAL

Failures, and the group only shrinks.

## Chaimae BOUTI

Resurrection and reinsertion.

## Luiz Eduardo Akazawa

Voluntary join and leave.
