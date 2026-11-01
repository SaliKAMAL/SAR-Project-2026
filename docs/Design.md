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

## Part 2: Failures and shrinking group

This stage introduces fail-stop actor crashes without resurrection. Surviving actors must maintain the totally-ordered multicast sequence and avoid deadlocks without relying on timeouts.

### 1. Perfect Failure Detector
The system implements a perfect failure detector relying directly on the underlying task runtime rather than network timeouts. 
A task terminates either explicitly via `kill()` or stochastically when `mayFail()` is true and a random failure is drawn. In both cases, the runtime deterministicly closes the queues and triggers the registered `Task.Listener`. 
This listener acts as the perfect detector: a member is announced dead only after its task is actually dead, and every death is immediately announced. Upon detection, it broadcasts a crash notice to all surviving actors.

### 2. Flush Mechanism
To maintain uniform agreement when a member disappears, survivors execute a synchronous flush phase before removing the crashed actor from their view:
* **Broadcast pending set:** Upon receiving a crash notice, a survivor does not immediately delete the member. Instead, it sends the others the set of all not-yet-delivered messages it currently holds from the crashed sender, bundled with its current logical clock.
* **Intersection rule:** Each survivor waits to receive this pending set from every other survivor. A survivor keeps a message from the crashed sender *if and only if* every survivor listed it in their set. 
* **Resolution:** The retained messages are delivered strictly in timestamp order. Any other message from the crashed sender is permanently dropped. 
* **View update:** Once resolved, the crashed ID formally leaves the view. The standard delivery stability rule resumes without waiting for acknowledgements from the dead actor.

### 3. Isolation and Verification
* Messages posted toward a dead member are naturally dropped, as the task runtime automatically closes that member’s queues upon `kill()`. Internal routing tasks remain non-failable.
* Once the flush is complete and the actor is isolated, the state change is signaled to the external monitor via a direct, synchronous call to `Verifier.crashed(actorId)`.

### 4. Tests
Validation is handled by `TestMulticastFailures`, which introduces two distinct crash scenarios:
1. **Forced Suicide:** An actor explicitly calls `kill()`.
2. **Random Failures:** An actor is configured with `setMayFail(true)` and a `Task.setFailureProbability` greater than 0.
Both scenarios are executed with a standard message period and then strictly repeated with `period = 0`. The verifier checks that all surviving actors reach the exact same delivery sequence. A timeout is configured to fail the test if any survivor deadlocks while waiting for a dead actor's acknowledgement. The test concludes successfully when remaining actors stabilize on the sequence and the dead actor delivers nothing further.