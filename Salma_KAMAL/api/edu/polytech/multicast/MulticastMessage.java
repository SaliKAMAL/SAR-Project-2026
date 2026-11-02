package edu.polytech.multicast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * One multicast payload, or an acknowledgement used only to advance stability.
 * Order is the Lamport timestamp, then the sender id.
 */
public final class MulticastMessage implements Comparable<MulticastMessage> {

  static final byte DATA = 1;
  static final byte ACK = 2;

  private final long messageId;
  private final String senderId;
  private final long generationId;
  private final long lamportTimestamp;
  private final byte[] payload;

  public MulticastMessage(long messageId, String senderId, long generationId, long lamportTimestamp,
      byte[] payload) {
    this.messageId = messageId;
    this.senderId = senderId;
    this.generationId = generationId;
    this.lamportTimestamp = lamportTimestamp;
    this.payload = payload.clone();
  }

  public long messageId() {
    return messageId;
  }

  public String senderId() {
    return senderId;
  }

  public long generationId() {
    return generationId;
  }

  public long lamportTimestamp() {
    return lamportTimestamp;
  }

  public byte[] payload() {
    return payload.clone();
  }

  @Override
  public int compareTo(MulticastMessage other) {
    int byTime = Long.compare(lamportTimestamp, other.lamportTimestamp);
    if (byTime != 0)
      return byTime;
    return senderId.compareTo(other.senderId);
  }

  static byte[] encodeData(MulticastMessage message) {
    try {
      ByteArrayOutputStream raw = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(raw);
      out.writeByte(DATA);
      out.writeLong(message.messageId);
      out.writeUTF(message.senderId);
      out.writeLong(message.generationId);
      out.writeLong(message.lamportTimestamp);
      out.writeInt(message.payload.length);
      out.write(message.payload);
      out.flush();
      return raw.toByteArray();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  static byte[] encodeAck(String senderId, long lamportTimestamp) {
    try {
      ByteArrayOutputStream raw = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(raw);
      out.writeByte(ACK);
      out.writeUTF(senderId);
      out.writeLong(lamportTimestamp);
      out.flush();
      return raw.toByteArray();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  static Object decode(byte[] bytes) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      byte kind = in.readByte();
      if (kind == DATA) {
        long messageId = in.readLong();
        String senderId = in.readUTF();
        long generationId = in.readLong();
        long lamportTimestamp = in.readLong();
        int length = in.readInt();
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new MulticastMessage(messageId, senderId, generationId, lamportTimestamp, payload);
      }
      if (kind == ACK)
        return new Ack(in.readUTF(), in.readLong());
      throw new IllegalArgumentException("unknown message kind " + kind);
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  static final class Ack {
    final String senderId;
    final long lamportTimestamp;

    Ack(String senderId, long lamportTimestamp) {
      this.senderId = senderId;
      this.lamportTimestamp = lamportTimestamp;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MulticastMessage))
      return false;
    MulticastMessage other = (MulticastMessage) obj;
    return messageId == other.messageId && generationId == other.generationId
        && lamportTimestamp == other.lamportTimestamp && senderId.equals(other.senderId)
        && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(messageId) ^ senderId.hashCode();
  }
}
