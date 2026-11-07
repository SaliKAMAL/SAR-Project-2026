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

  static final byte CRASH = 3;
  static final byte FLUSH = 4;
  
  static final byte REINSERT_REQUEST = 5;
  static final byte VIEW_FLUSH = 6;

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
    int bySender = senderId.compareTo(other.senderId);
    if (bySender != 0)
      return bySender;
    // Part 3: the order must be total. Equal timestamps from one sender are possible (messages that
    // waited during a view change), and a PriorityQueue does not keep equal elements in order.
    return Long.compare(messageId, other.messageId);
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

  static byte[] encodeAck(String senderId, long generationId, long lamportTimestamp) {
    try {
      ByteArrayOutputStream raw = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(raw);
      out.writeByte(ACK);
      out.writeUTF(senderId);
      out.writeLong(generationId);
      out.writeLong(lamportTimestamp);
      out.flush();
      return raw.toByteArray();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }
  
  static byte[] encodeCrash(String crashedId, long lamportTimestamp) {
	    try {
	      ByteArrayOutputStream raw = new ByteArrayOutputStream();
	      DataOutputStream out = new DataOutputStream(raw);
	      out.writeByte(CRASH);
	      out.writeUTF(crashedId);
	      out.writeLong(lamportTimestamp);
	      out.flush();
	      return raw.toByteArray();
	    } catch (IOException ex) {
	      throw new IllegalStateException(ex);
	    }
	  }
  
  static byte[] encodeFlush(String senderId, String crashedId, long lamportTimestamp, long[] pendingIds) {
	    try {
	      ByteArrayOutputStream raw = new ByteArrayOutputStream();
	      DataOutputStream out = new DataOutputStream(raw);
	      out.writeByte(FLUSH);
	      out.writeUTF(senderId);
	      out.writeUTF(crashedId);
	      out.writeLong(lamportTimestamp);
	      out.writeInt(pendingIds.length);
	      for (long id : pendingIds) {
	        out.writeLong(id);
	      }
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
        return new Ack(in.readUTF(), in.readLong(), in.readLong());
      if (kind == CRASH) {
          return new CrashNotice(in.readUTF(), in.readLong());
        }
        if (kind == FLUSH) {
          String senderId = in.readUTF();
          String crashedId = in.readUTF();
          long lamportTimestamp = in.readLong();
          int length = in.readInt();
          long[] pendingIds = new long[length];
          for (int i = 0; i < length; i++) {
            pendingIds[i] = in.readLong();
          }
          return new Flush(senderId, crashedId, lamportTimestamp, pendingIds);
        }
        if (kind == REINSERT_REQUEST)
            return new ReinsertRequest(in.readUTF(), in.readLong(), in.readLong());
        if (kind == VIEW_FLUSH)
            return new ViewFlush(in.readUTF(), in.readLong(), in.readUTF(), in.readLong(), in.readLong());
        throw new IllegalArgumentException("unknown message kind " + kind);
    } catch (IOException ex) {
    	throw new IllegalArgumentException(ex);
    }
  }
  
  static final class ReinsertRequest {
	    final String requesterId;
	    final long generationId;
	    final long lamportTimestamp;

	    ReinsertRequest(String requesterId, long generationId, long lamportTimestamp) {
	      this.requesterId = requesterId;
	      this.generationId = generationId;
	      this.lamportTimestamp = lamportTimestamp;
	    }
	  }

	  static final class ViewFlush {
	    final String senderId;
	    final long senderGeneration;
	    final String subjectId;
	    final long subjectGeneration;
	    final long lamportTimestamp;

	    ViewFlush(String senderId, long senderGeneration, String subjectId, long subjectGeneration,long lamportTimestamp) {
	      this.senderId = senderId;
	      this.senderGeneration = senderGeneration;
	      this.subjectId = subjectId;
	      this.subjectGeneration = subjectGeneration;
	      this.lamportTimestamp = lamportTimestamp;
	    }
	  }
	  
	  static byte[] encodeReinsertRequest(String requesterId, long generationId, long lamportTimestamp) {
		  try {
		      ByteArrayOutputStream raw = new ByteArrayOutputStream();
		      DataOutputStream out = new DataOutputStream(raw);
		      out.writeByte(REINSERT_REQUEST);
		      out.writeUTF(requesterId);
		      out.writeLong(generationId);
		      out.writeLong(lamportTimestamp);
		      out.flush();
		      return raw.toByteArray();
		    } catch (IOException ex) {
		      throw new IllegalStateException(ex);
		    }
		  }
	  
	 
	  /**
	   * Part 3: answer to a view change. The queues are FIFO, so when a member has the answer of
	   * another member, it also has every message that member sent before. It is a barrier.
	   */
	  static byte[] encodeViewFlush(String senderId, long senderGeneration, String subjectId,
	      long subjectGeneration, long lamportTimestamp) {
	    try {
	      ByteArrayOutputStream raw = new ByteArrayOutputStream();
	      DataOutputStream out = new DataOutputStream(raw);
	      out.writeByte(VIEW_FLUSH);
	      out.writeUTF(senderId);
	      out.writeLong(senderGeneration);
	      out.writeUTF(subjectId);
	      out.writeLong(subjectGeneration);
	      out.writeLong(lamportTimestamp);
	      out.flush();
	      return raw.toByteArray();
	    } catch (IOException ex) {
	      throw new IllegalStateException(ex);
	    }
	  }

  static final class Ack {
    final String senderId;
    final long generationId;
    final long lamportTimestamp;

    Ack(String senderId, long generationId, long lamportTimestamp) {
        this.senderId = senderId;
        this.generationId = generationId;
        this.lamportTimestamp = lamportTimestamp;
      }
  }
  
  public static final class CrashNotice {
	public final String crashedId;
    public final long lamportTimestamp;

    public CrashNotice(String crashedId, long lamportTimestamp) {
      this.crashedId = crashedId;
      this.lamportTimestamp = lamportTimestamp;
    }
  }

  public static final class Flush {
	public final String senderId;
	public final String crashedId;
    public final long lamportTimestamp;
    public final long[] pendingMessageIds; // The list of message IDs that were pending at the time of the flush, which may not have been delivered to all actors yet.

	public Flush(String senderId, String crashedId, long lamportTimestamp, long[] pendingMessageIds) {
	  this.senderId = senderId;
      this.crashedId = crashedId;
      this.lamportTimestamp = lamportTimestamp;
      this.pendingMessageIds = pendingMessageIds;
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
