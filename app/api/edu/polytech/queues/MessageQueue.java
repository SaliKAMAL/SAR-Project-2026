/*
 * Copyright (C) Pr. Olivier Gruber                                    
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
package edu.polytech.queues;

public interface MessageQueue {

  QueueBroker broker();

  public interface Listener {
    void received(byte[] msg);
    void closed();
  }
  void setListener(Listener l);

  public interface SendListener {
    void sent(byte[] bytes, int offset, int length);
  }
  
  boolean send(byte[] bytes, int offset, int length, SendListener l);

  void close();
  boolean closed();
}
