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

public interface QueueBroker {

  String getName();

  Task getTask();

  public interface BindListener {
    void accepted(MessageQueue queue);

    void unbound();
  }

  boolean bind(int port, BindListener listener);

  boolean unbind(int port);

  public interface ConnectListener {
    void connected(MessageQueue queue);

    void refused();
  }

  boolean connect(String name, int port, ConnectListener listener);

}
