package edu.polytech.multicast;

import java.util.Objects;

/** Immutable description of one voluntary join or leave request. */
public final class MembershipRequest {

  public enum Operation { JOIN, LEAVE }

  private final String requestId;
  private final Operation operation;
  private final String actorId;
  private final long generationId;
  private final long viewId;

  /** The caller supplies a unique request ID and reuses it when retrying the same request. */
  public MembershipRequest(String requestId, Operation operation, String actorId, long generationId, long viewId) {
    this.requestId = requireId(requestId);
    this.operation = Objects.requireNonNull(operation, "operation");
    this.actorId = requireId(actorId);
    if (generationId < 1)
      throw new IllegalArgumentException("generationId must be positive");
    if (viewId < 0)
      throw new IllegalArgumentException("viewId must not be negative");
    this.generationId = generationId;
    this.viewId = viewId;
  }

  public String requestId() { 
	  return requestId; 
  }
  public Operation operation() { 
	  return operation; 
  }
  public String actorId() { 
	  return actorId; 
  }
  public long generationId() { 
	  return generationId; 
  }
  public long viewId() { 
	  return viewId; 
  }

  static String requireId(String id) {
    if (id == null || id.isBlank())
      throw new IllegalArgumentException("ID must not be blank");
    return id;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MembershipRequest))
      return false;
    MembershipRequest request = (MembershipRequest) other;
    return requestId.equals(request.requestId) && operation == request.operation
        && actorId.equals(request.actorId) && generationId == request.generationId
        && viewId == request.viewId;
  }
}
