package edu.polytech.channels.local;

import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CChannel implements Channel {

  private final CircularBuffer in;
  private final CircularBuffer out;
  private final ConnectionState state;

  static class ConnectionState {
    volatile boolean disconnected;
  }

  CChannel(CircularBuffer in, CircularBuffer out, ConnectionState state) {
    this.in = in;
    this.out = out;
    this.state = state;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) {
    if (length == 0)
      return 0;
    int count = 0;
    synchronized (in) {
      // An empty buffer means the peer has not written yet, so wait for data.
      while (in.empty() && !state.disconnected) {
        try {
          in.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return 0;
        }
      }
      if (in.empty())
        return 0;
      while (count < length && !in.empty())
        bytes[offset + count++] = in.pull();
      in.notifyAll();
    }
    return count;
  }

  @Override
  public int write(byte[] bytes, int offset, int length) {
    if (length == 0)
      return 0;
    int count = 0;
    synchronized (out) {
      // A full buffer makes the writer wait until the reader removes bytes.
      while (count < length && !state.disconnected) {
        while (out.full() && !state.disconnected) {
          try {
            out.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return count;
          }
        }
        if (state.disconnected)
          break;
        while (count < length && !out.full())
          out.push(bytes[offset + count++]);
        out.notifyAll();
      }
    }
    return count;
  }

  @Override
  public boolean disconnected() {
    return state.disconnected;
  }

  @Override
  public void disconnect() {
    state.disconnected = true;
    synchronized (in) {
      in.notifyAll();
    }
    synchronized (out) {
      out.notifyAll();
    }
  }

}
