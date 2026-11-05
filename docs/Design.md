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

## Luiz Eduardo AKAZAWA NORA

### Voluntary join and leave

A new actor can join the running group, and a member can leave without crashing. Failures and reinsertion still work as in the previous stages. The message fields, ordering by `(timestamp, senderId)`, and normal acknowledgement rule stay the same.

A join or leave completes pending deliveries from the old view before installing the next view.

#### Membership requests

The new operations are `join(peerId)` for an actor in `CREATED`, and `leave()` for an actor in `IN_VIEW`. They run on the actor's private task. Invalid calls are rejected. Retries keep the same request id.

```
CREATED → JOINING → IN_VIEW → LEAVING → LEFT
```

A `JOINING` actor only exchanges the messages needed to join the group. A `LEAVING` actor stops sending new multicast messages but keeps receiving messages, sending acknowledgements, and delivering messages until its leave finishes. `LEFT` is terminal: new actors use new ids. A crashed actor uses Stage 3's `respawn()` and `reinsert()` instead.

`GroupManager` is only used at startup. A newcomer contacts an existing member through `QueueBroker`. That peer forwards the request. It cannot admit the newcomer alone.

Join, leave, and reinsertion requests use the multicast protocol so all members process them in the same order. Members perform one membership change at a time. These requests are handled internally instead of being delivered as multicast messages. Later requests wait and are checked against the actor’s state and the updated membership before being processed.

Each membership change increases viewId. Members keep the actor IDs and their generations, as in Stage 3. Each join or leave request identifies the current view, the request, the operation, and the actor with its generation. Replies from cancelled or earlier changes are ignored. Message IDs remain unique when new actors join.

#### Completing pending messages before a membership change

Join and graceful leave use the following flush. Reinsertion follows Stage 3. Join, leave, and reinsertion requests are processed one at a time.

1. Pause sending and delivering multicast messages. Keep receiving messages and sending acknowledgements. New multicast requests wait locally. The task never blocks its thread.
2. Each old member sends a marker marking the end of its multicast sends for the current view. It uses the same MessageQueue connection, after its earlier messages have been accepted. These queues preserve sending order. Receiving a marker from every other member confirms that all earlier multicast messages have arrived.
3. Exchange pending messages and clocks. Keep the union of pending messages, including those from a graceful leaver. Keep delivered ids too, to avoid duplicates.
4. Each current member, including the leaving actor, delivers the remaining messages from the current view in (timestamp, senderId) order, skipping messages it has already delivered. All members confirm that they have completed these deliveries before proceeding.
5. Collect clocks after finishing these deliveries. Let `V` be their maximum, also including the newcomer's clock when joining. All participants call `witness(V)`.
6. Record and confirm the membership decision using the steps below. Record the join or leave with the verifier and notify listeners before releasing waiting sends or enabling deliveries in the new view. Sends and initial acknowledgements run in new runnables, giving them timestamps greater than `V`.

Queues continue receiving messages during the change. Messages sent under the new membership wait until the receiving actor has updated its member list. Markers, acknowledgements, and membership requests are handled internally.

#### Confirming a join or leave

The participants are all current members, including a leaver, plus the newcomer for a join. They exchange the following messages through their queues. Each message identifies the request, original view, and proposed next view. Replies for the active exchange remain valid after local installation. Replies count once per actor and generation. Each participant counts its own reply locally.

1. Each participant sends `READY` to everyone after finishing its preparation. It includes the proposed member list and `V`. All replies must agree on these values.
2. A participant that has matching `READY` replies from everyone records a `DECISION` and sends it to all participants. The decision contains the next view id, member list, `V`, and the complete set of ready replies. Every participant computes the same decision for the selected request.
3. A participant accepts a decision only with the complete matching ready replies. It keeps the decision and sends `STORED` to everyone. Receiving the same decision again only repeats the acknowledgement.
4. After receiving `STORED` from every participant for that decision, an actor updates its membership, records the event with the verifier, and notifies its listener. It then sends `INSTALLED` and enables sends and deliveries allowed by its state. A leaver stops multicast sends and deliveries but continues processing membership replies until it receives `INSTALLED` from all remaining members and closes its queues. The next membership request waits until everyone has confirmed installation or crash recovery has resolved the change.

Members retain the decision for the current view and through any recovery for that change. If a participant crashes, they pause the change and exchange `CHANGE_STATUS` messages containing any recorded decision. The exchange identifies the request, original view, and surviving participants. Status messages for a different survivor set are ignored. A participant sends its status only after pausing. It stops creating decisions for that attempt, and late replies cannot advance it normally. An already recorded decision is included even if local membership has not changed yet.

Wait for status messages from every surviving participant. If any contains a valid decision, everyone keeps that decision and waits for storage confirmations from every surviving participant before completing the membership change. Membership notifications are recorded before any new deliveries. Multicast sends and deliveries remain paused until recovery and any required Stage 2 crash procedure finish. If none contains one, cancel the attempt. Then use Stage 2 for any actor that is a member after this outcome, and retry requests that remain valid. If another participant crashes during recovery, repeat the status exchange with the remaining survivors. Messages from earlier exchanges cannot complete the new exchange.

These steps apply to voluntary joins and leaves. Reinsertion still follows Stage 3. Each membership event is recorded once by the verifier, even when several members report it. Each local listener is notified once before deliveries in the new view. The verifier compares message ids, order, and payload for periods when actors shared membership, without requiring newcomers to receive earlier history.

#### Joining and leaving

For **`join(peerId)`**:

1. Create the actor's task, broker, empty pending queue, and acknowledgement table. Its generation starts at `1`. Register its id and connection details through the contacted peer, which submits the join request.
2. While `JOINING`, connect queues with every old member and wait for the request's turn. The old members run the flush among themselves, without sending their history to the newcomer.
3. Prepare the next view with the newcomer. It calls `witness(V)` and confirms that its queues are ready. Complete the decision exchange before updating membership. On local installation, set the newcomer to `IN_VIEW` and record the join before enabling multicast sends and deliveries.
4. In a new runnable, the newcomer sends an acknowledgement with a timestamp greater than `V` to everyone. The join notification uses `Verifier.joined(actorId)` and `GroupListener.joined`.

The newcomer ignores messages timestamped at or below `V`. From admission onwards, it follows the same delivery rule as the others.

For **`leave()`**:

1. Set the actor to `LEAVING`, stop its periodic sender, and submit the leave request. Messages already sent still belong to the old view.
2. Run the flush with every old member, including the leaver. Keep its pending messages and finish the same old deliveries everywhere.
3. Complete the decision exchange, then remove its id, generation, and acknowledgement entry from the next view. Remaining members call `witness(V)` and stop waiting for its acknowledgements.
4. On local installation, set the leaver to `LEFT` and stop its message deliveries. The leave notification uses `Verifier.left(actorId)` and `GroupListener.left`. Close its queues only after receiving `INSTALLED` from every remaining member.

The leaver keeps its delivered history but gets no later messages. Multicast messages and normal acknowledgements from actors outside the installed view are ignored. Replies from the original participants are still accepted for an unfinished membership exchange. If the last member leaves, it finishes its pending messages and leaves an empty, terminal group. Another group needs a new bootstrap.

#### Crashes during membership changes

Stage 2 handles actor crashes. If a crash interrupts a join or leave, survivors first resolve the membership decision using the procedure described above.

1. If no decision was retained, cancel the interrupted change. A crashed newcomer has not joined, and a crashed leaver remains a failed member.
2. If a decision was retained, complete the membership change, then apply Stage 2’s crash procedure if the crashed actor belongs to the resulting group.
3. If the peer introducing a newcomer crashes, the newcomer retries through another member using the same request ID. Pending membership requests are checked against the updated membership before being retried.
