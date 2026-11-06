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

What changes in the code:

- The `messageId` now contains the generation: `generation << 48 | actor index << 32 | sequence`.
- Acknowledgements also carry the `generationId`.
- Two new message kinds: `REINSERT_REQUEST` and `VIEW_FLUSH`.
- When two messages have the same timestamp and the same sender, the `messageId` decides the order.

The stability rule is the same. The crash flush of Part 2 is not reused: a reinsertion removes nobody, so it needs a simpler flush (section 4).

#### 1. Two steps, not one

"Alive" and "in the group" are not the same thing.

| Step | Method | Result |
|---|---|---|
| 1. Resurrection | `respawn()` | The actor is alive again, but **outside the view**. |
| 2. Reinsertion | `reinsert()` | The actor is **inside the view** and takes part in the multicast. |

```
IN_VIEW  --crash-->  DEAD  --respawn()-->  RESPAWNED  --reinsert()-->  IN_VIEW
```

- `respawn()` works only if the actor is `DEAD`. `reinsert()` works only if it is `RESPAWNED`. In any other state they throw `IllegalStateException`.
- `respawn()` does **not** wait for the crash flush of Part 2. If a survivor still has the actor in its view, it keeps the reinsertion request on hold until its crash flush is finished (section 6).
- `respawn()` returns at once. The caller waits for `state() == RESPAWNED`.

#### 2. Generation

`generationId` starts at `1`. Each `respawn()` adds `1`. It tells apart the old life and the new life of the same actor id.

- Every message and every acknowledgement carries the `generationId` of its sender.
- Each actor keeps, for every member of its view, the generation it accepts. A message or acknowledgement with another generation is **ignored**: it is not stored and not counted for stability. This is how late messages of the old life are dropped.
- The generation is also inside the `messageId`, so a message of the new life is never taken for a duplicate of the old life.
- Two exceptions to "the accepted generation comes from the view":
  - a `RESPAWNED` actor accepts the members it is connected to;
  - a member that runs a view change for actor X accepts the new generation of X, because X can be in its new view before the members are.

#### 3. Resurrection: `respawn()`

1. Create a **new task** and a **new broker** named `id@generation` (a broker name cannot be used twice). The failure detector watches the new task. Random failure is switched on again only when the setup is finished.
2. Connect to every member that is `IN_VIEW`. The connection uses a reserved "rejoin" port (1000 + actor index) that every actor opens at start. The members keep these new queues **apart** from their normal links, so nobody sends anything to the actor yet.
3. **Empty** the message queue, the acknowledgement table and the list of delivered ids. The new life does not replay the periodic load.
4. Increase `generationId` by 1.
5. Call `Verifier.resurrected(actorId)` and `GroupListener.resurrected`.
6. Set the state to `RESPAWNED`.

After this the actor is **not in the view**. The members do not send it anything and do not wait for its acknowledgements. It does not multicast and does not deliver anything. This is why the gap stays empty.

#### 4. Reinsertion: `reinsert()`

Reinsertion is a **view change**. Nobody is removed, so no message is dropped and we do not need sets of messages. The FIFO queues do the ordering: when a member has the answer of another member, it already has every message that member sent before. The answer works like a barrier.

1. **Request.** The respawned actor sends `REINSERT_REQUEST` (its id, its new generation, its clock) to every member. Then it sends its own `VIEW_FLUSH` answer.
2. **Freeze.** A member that gets the first message of this view change stops multicasting. If the application asks for a multicast, the payload waits in a list. The member sends `VIEW_FLUSH` (its clock) to every member and to the returning actor. It delivers nothing during the view change.
3. **Choose V.** A member is ready when it has the `VIEW_FLUSH` of every member and of the returning actor. `V` is the biggest clock among these answers. Every message sent before the view change has a timestamp `<= V`.
4. **Install.** The member calls `witness(V)`, delivers in order everything it holds with a timestamp `<= V`, adds the actor to the view with its new generation, and unfreezes.
5. **Announce.** In its next step, the member sends an acknowledgement to everybody, the returning actor included. (Without it, an acknowledgement sent before the install never reached the new member, and the new member could wait forever.) Then it sends the waiting payloads, one per step, so each has a different timestamp, greater than `V`.
6. **The returning actor** does the same when it has all the answers: `witness(V)`, removes from its queue every message `<= V`, installs the view, calls `Verifier.reinserted` and `GroupListener.reinserted`, becomes `IN_VIEW`, and sends its announce acknowledgement.

#### 5. What the reinserted actor delivers

Only messages with a timestamp **greater than `V`**. It never receives a copy of the old history. A message `<= V` is removed from its queue when it installs the view. From then on it follows the normal stability rule, and its sequence is the end of the sequence of the other members.

#### 6. Interaction with other events

- **A member crashes during a view change.** We stop waiting for its answer and the view change goes on. It is not restarted.
- **A reinsertion request arrives while the crash flush of that same actor is still running.** The request waits and is handled when the crash flush ends.
- **Several view changes at once.** One at a time. The other messages wait and are replayed after the current one.
- **The returning actor dies during its own reinsertion.** *Known limitation:* not fully handled yet (about 1 run in 60 gets stuck).

#### 7. Verifier and tests

The verifier gets two events: `resurrected(actorId)` and `reinserted(actorId)`.

At `resurrected`, it records the ids already delivered by the other actors (the **absent list**) and where the new life starts. `checkReinserted` then verifies that the new life:

- delivers nothing from the absent list;
- delivers nothing twice;
- is exactly the end of the sequence of the survivors.

`TestMulticastReinsertion` covers: suicide and random failure, with a long period and with period `0`; two reinsertions in a row; a respawned actor left outside the view; reinsertion under heavy load; respawn before the crash flush has ended; calls in the wrong state.

## Luiz Eduardo Akazawa

Voluntary join and leave.
