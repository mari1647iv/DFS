# Distributed Systems Group Project 2
# Distributed File System
  Students: Kamil Shakirov, Nataliya Tupikina, Marina Ivanova (BS18)

## System Setup
  + Install docker
  + Install postgres 
  + Run postgres container on namenode
  + Run next commands:
      + Namenode:
        + ```sudo docker run -i -t --network='host' deadman445/namenode:latest <your_namenode_ip> <your_namenode_host>```
      + Datanode:
        + ```sudo docker run --network='host' -t deadman445/datanode:latest 10.0.15.10 <your_datanode_host> <your_datanode_ip> <your_namenode_host>```
## Architectural diagram
![Chat](https://github.com/mari1647iv/DFS/blob/main/ArchitecturalDiagram.png)
  In this project we decided to implement Distributed File System using recursive model anology. Client sends commands to name server that passes it to storage servers, and vice versa.
## Communication protocols
  This DFS implementation uses TCP protocols for communication and data transfer.
## Team members contribution

  + Kamil Shakirov: Data Servers
  + Marina Ivanova: Name Server, documentation
  + Nataliya Tupikina: Client Application, Docker deployment
  
## Challenges faced
  + Architecture structure model choosing
  + Issues with commmunication between project parts
  + Some problems with time management
