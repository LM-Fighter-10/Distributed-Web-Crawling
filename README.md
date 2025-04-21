# Distributed Web Crawler & Indexer

 This project implements a basic distributed web crawling and indexing system using MPI (Message Passing Interface) with Python. The system is structured into three primary node types:

- **Master Node** – Coordinates the system, assigns URLs for crawling, collects new URLs, and manages node tasks.
- **Crawler Node(s)** – Fetch and process web pages by crawling assigned URLs, extracting new URLs and content.
- **Indexer Node** – Receives processed content from crawlers, performs indexing operations, and sends status updates back to the master.

The overall design reflects a distributed architecture suitable for scaling web crawling and indexing tasks.

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Components](#components)
  - [Master Node](#master-node)
  - [Crawler Node](#crawler-node)
  - [Indexer Node](#indexer-node)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)
- [Configuration and Extensibility](#configuration-and-extensibility)
- [Final Technology Stack Selections](#final-technology-stack-selections)
- [Logging and Debugging](#logging-and-debugging)
- [Future Enhancements](#future-enhancements)

---

## Overview

This project demonstrates a distributed system where web crawling and indexing tasks are decoupled into separate processes communicating over MPI. The master node distributes crawling tasks to crawler nodes, aggregates discovered URLs, and potentially passes web content to an indexer node. Although the functionality is currently simulated (e.g., using sleep delays instead of actual HTTP requests or database interactions), the architecture provides a framework that can be extended to include real web crawling (with libraries such as `requests`, `BeautifulSoup4`, or `scrapy`) and indexing (with libraries like Elasticsearch).

---

## System Architecture

The system is built with the following design:

- **Distributed Processing:** Uses `mpi4py` for communication between nodes.
- **Task Distribution:** The master node maintains a URL queue, assigns tasks to crawler nodes in a round-robin manner, and collects new URLs from crawlers.
- **Indexing:** Indexer nodes receive content from crawler nodes to perform indexing. In this skeleton, the indexing is simulated with a delay.
- **Message Tags:** Different MPI message tags are used to differentiate between task assignments, status updates, and error reporting:
  - **Tag 0:** Master sends URL crawling tasks.
  - **Tag 1:** Crawler returns extracted URLs.
  - **Tag 2:** Crawler (or future task routines) sends content to indexer nodes.
  - **Tag 99:** Status/heartbeat messages.
  - **Tag 999:** Error reports.

For more details on the individual node implementations, see the comments within the source files:
- **Indexer Node:** [indexer_node.py](&#8203;:contentReference[oaicite:0]{index=0})
- **Master Node:** [master_node.py](&#8203;:contentReference[oaicite:1]{index=1})
- **Crawler Node:** [crawler_node.py](&#8203;:contentReference[oaicite:2]{index=2})

---

## Components

### Master Node

- **Role:**  
  - Initiates the system and coordinates tasks.
  - Maintains the URL queue and assigns URLs to crawler nodes.
  - Processes responses from crawler nodes, such as the list of newly extracted URLs.
- **Key Logic:**
  - Uses non-blocking checks (`MPI.iprobe`) to monitor message queues.
  - Assigns tasks in a round-robin fashion.
  - Logs key activities and error messages.
- **File:** [master_node.py](&#8203;:contentReference[oaicite:3]{index=3})

### Crawler Node

- **Role:**
  - Listens for URL crawling tasks from the master node.
  - Simulates the fetching of web pages with a delay and generates example extracted URLs.
  - Returns extracted URLs to the master and sends status updates.
- **Key Logic:**
  - Simulated using `time.sleep` to represent network delay.
  - Demonstrates message sending back to the master using different MPI tags.
- **File:** [crawler_node.py](&#8203;:contentReference[oaicite:4]{index=4})

### Indexer Node

- **Role:**
  - Waits for content from crawler nodes to be indexed.
  - Simulates an indexing process by introducing a delay.
  - Sends back confirmations or error messages to the master.
- **Key Logic:**
  - Placeholder comments indicate where real indexing (e.g., Elasticsearch) would be implemented.
  - Uses MPI communications to report indexing status.
- **File:** [indexer_node.py](&#8203;:contentReference[oaicite:5]{index=5})

---

## Installation and Setup

### Prerequisites

- **Python 3.6+**
- **MPI Implementation** (e.g., OpenMPI or MPICH)
- **mpi4py Library**

### Installation Steps

1. **Install MPI**  
   Follow your platform’s instructions to install OpenMPI or a compatible MPI library.

2. **Install Python and Dependencies**

   It is recommended to use a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. **Install mpi4py:**
  ```bash
  pip install mpi4py
  ```
4. **Optional Libraries: If you plan to integrate actual web crawling or indexing functionality, install:**
  - `requests`
  - `beautifulsoup4`
  - `scrapy`
  - `elasticsearch`
  ```bash
  pip install requests beautifulsoup4 scrapy elasticsearch
  ```

## Usage
### Running the System
The distributed system is launched using MPI. Make sure that you run at least three nodes (one master, one crawler, and one indexer). The following example uses 4 nodes (1 master, 2 crawlers, and 1 indexer):
```bash
mpiexec -n 4 python master_node.py
```

### Note:
- The master node (running from `master_node.py`) is responsible for assigning tasks.
- The other processes (identified by their rank in MPI) will run either crawler or indexer logic as per their rank. Make sure your MPI process count meets the assumptions in the code (master at rank 0, crawler nodes following, and indexer nodes at higher ranks).

### Message Tags Overview
- Tag 0: Used by the master node to send URL crawl tasks to crawler nodes.
- Tag 1: Used by crawler nodes to send extracted URLs back to the master.
- Tag 2: Intended (but currently commented) for sending extracted content to the indexer.
- Tag 99: For status and heartbeat messages.
- Tag 999: For error messages.

## Configuration and Extensibility
### Extending Crawling Capability
- Real Crawling: Replace the sleep calls in `crawler_node.py` with actual HTTP requests. Use libraries such as `requests` or `scrapy` for fetching HTML content.
- URL Extraction: Implement proper link extraction by parsing the HTML DOM.

### Enhancing Indexing Functionality
- Real Indexing: Instead of simulating indexing with `time.sleep`, integrate Elasticsearch in `indexer_node.py` for real-time, distributed full-text indexing and analytics.
- Database Integration: Connect the indexer to a persistent database for storing indexed documents.

### Master Node Improvements
- Distributed Task Queue: Replace the simple list-based URL queue with a more robust distributed task queue system (e.g., using Celery with Redis).
- Load Balancing: Enhance task assignment logic to account for load variations across crawler nodes.

## Final Technology Stack Selections
- ☁️ Cloud Provider: Google Cloud Platform (GCP)
Scalable, managed services with strong integration and reliability.
- 🗃️ Database: MongoDB (accessed via mongosh)
Flexible NoSQL database for storing semi-structured crawled content.
- 🕷️ Web Crawling: Scrapy
Asynchronous, powerful crawling framework ideal for large-scale scraping.
- 🔍 Indexing: Elasticsearch
Real-time, distributed search engine for full-text indexing and analytics.
- 📬 Task Queue: Celery with Redis
Efficient and scalable background task processing.
- 🗂️ Storage: Google Cloud Storage
Durable and scalable object storage for raw HTML, images, and documents.

## Logging and Debugging
Each node uses the built-in logging module configured at the INFO level. Logs include timestamps and node identities to facilitate monitoring of actions and errors. Adjust logging levels or redirect logs to files as needed.

## Future Enhancements
- Robust Shutdown Handling: Implement proper shutdown signals and graceful termination of nodes.
- Error Handling Enhancements: Develop more resilient error handling and retries in case of network or processing failures.
- Scalability Improvements: Investigate distributed systems solutions to handle high volumes of crawl and indexing tasks.
- Integration with Real Web Crawlers/Indexers: Replace placeholder code with robust, production-quality libraries.
