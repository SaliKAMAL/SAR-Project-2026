package edu.polytech.multicast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class MembershipMessage {
  public enum Type { JOIN_REQUEST, REQUEST, REJECT, FLUSH, READY, CONFIRM, STATUS }

  public final Type type;
  public final MembershipRequest request;
  public final String senderId;
  public final long generationId;
  public final Map<String, Long> members;
  public final Map<String, Long> survivors;
  public final long clock;
  public final boolean applied;

  public MembershipMessage(Type type, MembershipRequest request, String senderId, long generationId, Map<String, Long> members, Map<String, Long> survivors, long clock, boolean applied) {
    this.type = type;
    this.request = request;
    this.senderId = senderId;
    this.generationId = generationId;
    this.members = new HashMap<>(members);
    this.survivors = new HashMap<>(survivors);
    this.clock = clock;
    this.applied = applied;
  }

  static boolean isMembershipMessage(byte[] bytes) {
    return bytes.length > 0 && bytes[0] >= 7 && bytes[0] < 7 + Type.values().length;
  }

  public byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      
      out.writeByte(7 + type.ordinal());// MulticastMessage types goes from 1 to 6
      out.writeUTF(request.requestId());
      out.writeUTF(request.operation().name());
      out.writeUTF(request.actorId());
      out.writeLong(request.generationId());
      out.writeLong(request.viewId());
      out.writeUTF(senderId);
      out.writeLong(generationId);
      writeMembers(out, members);
      writeMembers(out, survivors);
      out.writeLong(clock);
      out.writeBoolean(applied);
      
      return bytes.toByteArray();
    } catch (IOException error) {
      throw new IllegalStateException(error);
    }
  }

  public static MembershipMessage decode(byte[] bytes) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      int type = in.readByte() - 7;
      
      if (type < 0 || type >= Type.values().length)
        throw new IllegalArgumentException("unknown membership message");
      
      MembershipRequest request = new MembershipRequest(in.readUTF(), MembershipRequest.Operation.valueOf(in.readUTF()), in.readUTF(), in.readLong(), in.readLong());
      String sender = in.readUTF();
      long generation = in.readLong();
      Map<String, Long> members = readMembers(in);
      Map<String, Long> survivors = readMembers(in);
      return new MembershipMessage(Type.values()[type], request, sender, generation, members, survivors, in.readLong(), in.readBoolean());
    } catch (IOException error) {
      throw new IllegalArgumentException("incomplete membership message", error);
    }
  }

  private static void writeMembers(DataOutputStream out, Map<String, Long> members) throws IOException {
    out.writeInt(members.size());
    for (Map.Entry<String, Long> member : members.entrySet()) {
      out.writeUTF(member.getKey());
      out.writeLong(member.getValue());
    }
  }

  private static Map<String, Long> readMembers(DataInputStream in) throws IOException {
    Map<String, Long> members = new HashMap<>();
    int count = in.readInt();
    for (int i = 0; i < count; i++)
      members.put(in.readUTF(), in.readLong());
    return members;
  }
}
