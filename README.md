# Distributed Systems Group Project 2
# Distributed File System
  Students: Kamil Shakirov, Nataliya Tupikina, Marina Ivanova (BS18)

## System Setup
  + Install docker
  + Install postgres or run ```docker run --name postgresDFS -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres``` on namenode machine
  + Run next commands:
      + Namenode:
        + ```docker run --network="host" urbeingwatched8/namenode <ip address> <port> <port for client>```
      + Datanode:
        + ```docker run --network="host" urbeingwatched8/datanode <ip address> <port> <ip addr of namenode> <port of name>```
      + Client:
        + ```python3 client.py <ip of name> <port of name for client>```
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
