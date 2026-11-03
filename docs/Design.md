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

### Failures, and the group only shrinks

This stage extends the protocol to tolerate fail-stop crashes while maintaining Uniform Agreement. The group only shrinks; resurrection is not permitted (`generationId` remains `1`).

**Perfect Failure Detector**
The system implements a Class $\mathcal{P}$ (Perfect) failure detector without relying on network timeouts. It hooks directly into the deterministic task runtime. A `Task.Listener` is registered on every actor's task. When a task terminates—either explicitly via `kill()` or stochastically when `mayFail` is true—the runtime invokes `failed`. This listener instantly acts as the detector and broadcasts a `CrashNotice` to all surviving actors, ensuring strong completeness and strong accuracy.

**The Flush and Intersection Protocol**
To prevent deadlocks and ensure that all correct actors deliver the exact same sequence of messages from a crashed sender, a survivor does not immediately remove the crashed member from its local view. Instead, it executes a synchronous flush phase:
1. **Pending Set Broadcast:** Upon receiving a `CrashNotice` for $p_k$, a survivor $p_i$ halts normal delivery of $p_k$'s messages. It broadcasts a `Flush` message containing its current logical clock and the exact set of not-yet-delivered messages it holds from $p_k$.
2. **Absolute Intersection:** $p_i$ waits until it has received a `Flush` message from every other survivor in the current view. It then computes the mathematical intersection of all received sets. A message from $p_k$ is retained *if and only if* every survivor listed it.
3. **Delivery and Purge:** Retained messages from $p_k$ are delivered strictly in timestamp order. Any message from $p_k$ not present in the absolute intersection is permanently dropped.

**View Reconfiguration and Isolation**
Once the flush is resolved, the crashed ID is officially removed from the `Group` view. The primary stability rule resumes seamlessly without waiting for acknowledgements from the dead actor. Messages posted toward the dead member are naturally dropped at the transport level, as the task runtime explicitly closes a member's message queues upon `kill()`. 
Upon removing the actor from the view, the survivor synchronously calls `Verifier.crashed(actorId)`.

**Validation (TestMulticastFailures)**
The protocol is validated under two strict scenarios:
1. **Forced Suicide:** An actor explicitly invokes `kill()`.
2. **Stochastic Faults:** An actor is initialized with `setMayFail(true)` and a `failureProbability > 0` (internal tasks remain non-failable).
Both scenarios are executed with a standard message period and then strictly repeated in burst mode (`period = 0`). The test passes when the dead actor ceases delivery, the `Verifier` confirms all surviving actors reached the exact same delivery sequence, and the test terminates without triggering the deadlock timeout.

## Chaimae BOUTI

### Resurrection and reinsertion

This stage adds one thing: a dead actor can come back. It comes back **empty**. The messages it missed while it was dead stay missed forever.

Nothing else changes. The message has the same five fields. The stability rule is the same. The flush from Part 2 is reused. 

#### 1. Two steps

Coming back is split in two steps, because "alive" and "in the group" are not the same thing.

| Step | Method | Result |
|---|---|---|
| 1. Resurrection | `respawn()` | The actor is alive again, but **outside the view**. |
| 2. Reinsertion | `reinsert()` | The actor is **inside the view** and takes part in the multicast. |

An actor is always in one of these states:

```
IN_VIEW  --crash-->  DEAD  --respawn()-->  RESPAWNED  --reinsert()-->  IN_VIEW
```

`respawn()` is only allowed when the actor is `DEAD`, and only after the crash flush of Part 2 is finished (the survivors have removed it and `Verifier.crashed` was called). `reinsert()` is only allowed when the actor is `RESPAWNED`.

#### 2. Generation

`generationId` starts at `1`. Each `respawn()` adds `1`. It tells apart the "old life" and the "new life" of the same actor id.

- Every message and every acknowledgement carries the `generationId` of its sender.
- Each actor keeps, for every member id, the generation it accepts now. This value comes from the current view.
- If the generation of a message or acknowledgement is **not** the accepted one, the message is **ignored**. It is not stored and it is not counted for stability.

This is how late messages from the previous life are dropped. A message sent by generation 1 can still arrive after the actor is back as generation 2. It is simply ignored.

#### 3. Resurrection: `respawn()`

`respawn()` does these things, in this order:

1. Create a **new task** for the same actor id. The old task is dead and never used again. The new task has a fresh clock. 
2. Create **new queues** between the actor and every member of the current view. The old queues were closed by `kill()` and cannot be reused.
3. **Empty** the actor's message queue and its acknowledgement table.
4. Increase `generationId` by 1.
5. Call `Verifier.resurrected(actorId)`.
6. Set the state to `RESPAWNED`.

After this, the actor is **not in the view**:

- The other members do not send it anything and do not wait for its acknowledgements. Their delivery goes on as before.
- The actor does not multicast and does not deliver anything.

This is why the gap stays empty. While the actor is outside the view, nobody gives it messages.

#### 4. Reinsertion: `reinsert()`

Reinsertion is a **view change**. It is ordered with the messages by the same flush as a crash. The only difference is that nobody is removed: every member of the old view is alive, so no message is dropped.

Steps:

1. **Request.** The respawned actor sends a reinsertion request to every member of the view. The request carries its id and its new `generationId`.
2. **Flush.** Each member answers like in a crash flush: it sends the set of messages it holds and has not delivered yet, together with its clock.
3. **Agree on what is delivered before the change.** When a member has the sets of everyone, it delivers the **union** of the sets, in order `(timestamp, senderId)`. Every sender is alive, so every message will reach everybody anyway. This is different from a crash, where we keep only what every survivor has.
4. **Choose the view timestamp `V`.** `V` is the biggest clock received during the flush. Each member calls `witness(V)`. The reinserted actor does the same, so its clock is at least `V`. Every message sent after the change has a timestamp **greater than `V`**.
5. **Install the new view.** The view now contains the actor with its new generation. The members start to wait for its acknowledgements, like for any other member.
6. **Announce.** The reinserted actor sends an acknowledgement with a timestamp greater than `V` to everybody, so nobody is blocked waiting for it. Then `Verifier.reinserted(actorId)` and `GroupListener.reinserted` are called, and the state becomes `IN_VIEW`.

While the flush runs, a member may receive new messages from other members. It stores them but does not deliver them until the new view is installed.

#### 5. What the reinserted actor delivers

The reinserted actor delivers **only messages with a timestamp greater than `V`**. It never receives a copy of the old history. A message with a timestamp lower than or equal to `V` that reaches it is ignored.

From the new view on, it follows the normal stability rule, like every other member, and its sequence is the same as theirs.

#### 6. Interaction with other events

- **A member crashes during the flush.** The crash flush of Part 2 runs first. The reinsertion is started again with the new view.
- **The respawned actor dies again before it is in the view.** The reinsertion is cancelled. The next `respawn()` uses a higher generation.

#### 7. Verifier and tests

The verifier gets two events: `resurrected(actorId)` and `reinserted(actorId)`.

For each reinserted actor, the verifier keeps the **absent list**: the ids delivered by the correct actors from the crash until the reinsertion view. It checks:

- the reinserted actor **never** delivers an id from the absent list;
- from the reinsertion view on, its sequence is the same as the other actors' sequence.

## Luiz Eduardo Akazawa

Voluntary join and leave.
