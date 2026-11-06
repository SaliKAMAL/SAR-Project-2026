package edu.polytech.multicast.tests;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import edu.polytech.multicast.MulticastMessage;

/**
 * The delivery order must not depend on the order in which messages reach an actor.
 * Two messages of the same sender can have the same timestamp (for instance messages that waited
 * during a view change). A PriorityQueue does not keep equal elements in insertion order, so the
 * order of messages must be total: timestamp, then sender id, then message id.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestMessageOrder
 */
public class TestMessageOrder {

  public static void main(String[] args) {
    try {
      sameOrderWhateverTheArrivalOrder();
      timestampThenSenderThenId();
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("message order tests: PASS");
    System.exit(0);
  }

  private static MulticastMessage message(long id, String sender, long timestamp) {
    return new MulticastMessage(id, sender, 1L, timestamp, ("m" + id).getBytes());
  }

  private static List<MulticastMessage> sample() {
    List<MulticastMessage> messages = new ArrayList<MulticastMessage>();
    long id = 1;
    for (long timestamp = 3; timestamp <= 5; timestamp++) {
      for (String sender : new String[] { "A", "B", "C" }) {
        for (int copy = 0; copy < 4; copy++) // same sender, same timestamp
          messages.add(message(id++, sender, timestamp));
      }
    }
    return messages;
  }

  private static void sameOrderWhateverTheArrivalOrder() {
    List<MulticastMessage> messages = sample();
    List<Long> reference = null;
    Random random = new Random(42);
    for (int run = 0; run < 300; run++) {
      List<MulticastMessage> shuffled = new ArrayList<MulticastMessage>(messages);
      Collections.shuffle(shuffled, random);
      PriorityQueue<MulticastMessage> queue = new PriorityQueue<MulticastMessage>();
      for (MulticastMessage message : shuffled)
        queue.add(message);
      List<Long> order = new ArrayList<Long>();
      while (!queue.isEmpty())
        order.add(queue.poll().messageId());
      if (reference == null)
        reference = order;
      else if (!reference.equals(order))
        throw new AssertionError("the delivery order depends on the arrival order (run " + run + ")");
    }
  }

  private static void timestampThenSenderThenId() {
    expectBefore(message(9, "B", 4), message(1, "A", 5), "timestamp first");
    expectBefore(message(9, "A", 5), message(1, "B", 5), "then sender id");
    expectBefore(message(1, "A", 5), message(2, "A", 5), "then message id");
  }

  private static void expectBefore(MulticastMessage first, MulticastMessage second, String rule) {
    if (first.compareTo(second) >= 0 || second.compareTo(first) <= 0)
      throw new AssertionError("wrong order: " + rule);
  }
}

