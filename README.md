# 📦 Distributed File Storage System

A distributed system for storing, fragmenting, replicating, and retrieving binary files across multiple storage nodes. The system operates without a central coordinator, and any node can accept client requests for upload or download.

✅ Distributed architecture  
✅ Fault tolerance through redundancy  
✅ File recovery even if a node is offline  
✅ Fully interactive Java client  

---

## 🎯 Overview

This project demonstrates fundamental distributed systems concepts:

- Files are **split into fragments**
- Fragments are **distributed across multiple nodes**
- Each node operates **independently**
- The client connects to **any chosen node**
- Files can be **uploaded, listed, and downloaded**
- The system remains functional **even if a node fails**

---

## 🏗️ Architecture

```
+---------+         +-----------+
| Client  | <--->   | Node 1    |
+---------+         +-----------+
                        |
                        v
                   +-----------+
                   | Node 2    |
                   +-----------+
                        |
                        ...
                        |
                   +-----------+
                   | Node N    |
                   +-----------+
```

- All nodes share the same codebase
- No master/coordinator node
- Nodes communicate directly
- The client interacts with any node

---

## 🔀 Fragmentation & Replication

When a file is uploaded:

1. The receiving node splits it into **N fragments** (N = number of nodes)
2. Each fragment receives a **SHA-256 hash**
3. Each node stores **two fragments**:
   - Its own fragment
   - The next fragment (cyclic redundancy)
4. Metadata is shared using a **manifest**, containing:
   - `fileId`
   - Original filename
   - Total fragments

✅ Each fragment exists in **two nodes**  
✅ The system tolerates the failure of **one node**  

---

## ♻️ File Reconstruction (Download)

When downloading:

1. The node loads the manifest
2. Determines required fragments
3. Fetches missing fragments from peers
4. Reassembles the file in order
5. Validates integrity using SHA-256
6. Sends the file back to the client

✅ Works even if one node is offline

---

## 🧰 Technologies

- Java
- TCP / HTTP-based communication
- SHA-256 hashing
- Local filesystem storage
- Base64 transfer for internal communication

---

## 📁 Project Structure

```
distributed-file-storage/
├── client/
│   ├── src/Client.java
│   ├── out/
│   └── downloads/
│
├── storage-node/
│   ├── src/StorageNode.java
│   ├── out/
│   └── data/
│
├── examples/
├── README.md
└── .gitignore
```

---

## ▶️ Running the System

### ✅ 1. Compile the storage node

```bash
cd storage-node
javac -d out src/StorageNode.java
```

### ✅ 2. Start multiple nodes

Example with 5 nodes:

```bash
java -cp out StorageNode 1 5001
java -cp out StorageNode 2 5002
java -cp out StorageNode 3 5003
java -cp out StorageNode 4 5004
java -cp out StorageNode 5 5005
```

### ✅ 3. Compile the client

```bash
cd client
javac -d out src/Client.java
```

### ✅ 4. Run the client

```bash
java -cp out Client
```

---

## 🖥️ Client Features

Interactive menu:

```
0 - Exit
1 - Test server
2 - List files on node
3 - Upload file to node
4 - Download file from node
```

✅ Select node by port  
✅ Browse local directories  
✅ Upload any file type  
✅ Download files by index  
✅ Saved automatically to `client/downloads/`

---

## ✅ Confirmed Capabilities

- Upload of text and image files
- Listing available files on any node
- File reconstruction and validation
- Successful download with one node offline
- Persistent data per node

---

## 🏅 Distributed System Concepts

- Decentralization
- Fragmentation
- Replication
- Redundancy
- Fault tolerance
- Peer-to-peer communication
- Client/server interaction

---

## 📌 Author

Hiago L. Silva  
Computer Science  

---

## ✅ Summary

This project successfully implements a distributed file storage system with:

- Fragmentation
- Replication
- Independent nodes
- Full client interaction
- Fault-tolerant recovery

It provides a practical demonstration of key distributed systems concepts and serves as a solid foundation for further development.
