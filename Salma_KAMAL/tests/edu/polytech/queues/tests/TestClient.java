/*
 * Copyright (C) 2026 Pr. Olivier Gruber                                    
 *                                                                       
 * This program is free software: you can redistribute it and/or modify  
 * it under the terms of the GNU General Public License as published by  
 * the Free Software Foundation, either version 3 of the License, or     
 * (at your option) any later version.                                   
 *                                                                       
 * This program is distributed in the hope that it will be useful,       
 * but WITHOUT ANY WARRANTY; without even the implied warranty of        
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         
 * GNU General Public License for more details.                          
 *                                                                       
 * You should have received a copy of the GNU General Public License     
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.polytech.queues.tests;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;

/**
 * A simple client that connects one or more time to the server. Each connection
 * is called a session, during which this client sends to the server a certain
 * number of bytes, split in small messages and awaits that the server echoes
 * them all back.
 */
public class TestClient {

  // the size of one small message:
  static final int MESSAGE_SIZE = 16;

  static boolean VERBOSE = true;

  static void log(String s) {
    if (VERBOSE)
      System.out.println(s);
  }

  String connectName;
  int connectPort;
  int nconnects;
  int nbytes;
  int clientNo;
  int connectNo;
  String name;
  Task task;
  QueueBroker broker;
  ClientSession session;

  TestClient(QueueBroker qb, int no, String name, int port, int nconnects, int nbytes) {
    this.name = "Client[" + no + "]";
    log(name+" started...");
    this.clientNo = no;
    this.connectName = name;
    this.connectPort = port;
    this.nconnects = nconnects;
    this.nbytes = nbytes;
    this.broker = qb;
    Task t = Task.task();
    this.task = t.newTask(this.name);
    this.task.post(()->{
      log(name+" connecting...");
      connect();
    });
  }

  public Task task() {
    return task;
  }

  private void connect() {
    if (connectNo >= nconnects) {
      System.out.println(name + ": done.");
      task.exit(null);
      return;
    }
    connectNo++;
    broker.connect(connectName, connectPort, new QueueBroker.ConnectListener() {

      @Override
      public void connected(MessageQueue queue) {
        log(name+" connected...");
        session = new ClientSession(queue, nbytes);
        task.post(()->{
          session.send();
        });
      }

      @Override
      public void refused() {
        System.out.println(name + ": connection refused.");
        task.exit(null);
        return;
      }
    });
  }

  class ClientSession implements MessageQueue.Listener {
    MessageQueue queue;
    int txOffset;
    int rxOffset;
    byte bytes[];

    ClientSession(MessageQueue queue, int length) {
      this.queue = queue;
      queue.setListener(this);
      bytes = new byte[length];
      for (int i = 0; i < length; i++)
        bytes[i] = (byte) i;
      log(name + ": has " + bytes.length + " bytes to send");
    }

    @Override
    public void received(byte[] bytes) {
      log(name + ": received " + bytes.length + " bytes, range [" + rxOffset + ":" + (rxOffset + bytes.length) + "[");
      for (int i = 0; i < bytes.length; i++)
        if (bytes[i] != (byte) (rxOffset + i))
          throw new Error();
      rxOffset += bytes.length;
      send();
    }

    @Override
    public void closed() {
      log(name + ": closed session");
      connect();
    }

    private void send() {
      if (txOffset >= bytes.length) {
        log(name + ": done, closing queue...");
        queue.close();
        return;
      }
      int size = 64;
      if (txOffset + size > bytes.length)
        size = bytes.length - txOffset;

      queue.send(bytes, txOffset, size, new MessageQueue.SendListener() {
        @Override
        public void sent(byte[] bytes, int offset, int length) {
          log(name + ": returned range [" + offset + ":" + (offset + length) + "[");
        }
      });
      log(name + ": sent range [" + txOffset + ":" + (txOffset + size) + "[");
      txOffset += size;
    }

  }
}
