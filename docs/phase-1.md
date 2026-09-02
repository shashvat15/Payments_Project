# Phase 1: Basic Synchronous Backend Architecture

## Table of Contents
1. [Project Overview & Architecture](#1-project-overview--architecture)
2. [Why We Have Two Services](#2-why-we-have-two-services)
3. [Why Each Service Has Its Own Database (Database-per-Service)](#3-why-each-service-has-its-own-database-database-per-service)
4. [Why Order Service Must Not Access `payment_db` Directly](#4-why-order-service-must-not-access-payment_db-directly)
5. [Why REST / HTTP Is Used Initially](#5-why-rest--http-is-used-initially)
6. [What Synchronous Communication Means](#6-what-synchronous-communication-means)
7. [What `@Transactional` Guarantees (Single-Database ACID)](#7-what-transactional-guarantees-single-database-acid)
8. [What `@Transactional` DOES NOT Guarantee Across Services](#8-what-transactional-does-not-guarantee-across-services)
9. [Failure Scenario 1: Payment Service Is Unavailable](#9-failure-scenario-1-payment-service-is-unavailable)
10. [Failure Scenario 2: Payment Succeeds but Updating the Order Fails (Dual-Write / Partial Failure)](#10-failure-scenario-2-payment-succeeds-but-updating-the-order-fails-dual-write--partial-failure)
11. [Why These Limitations Motivate Kafka, Saga, Outbox, and Resilience4j](#11-why-these-limitations-motivate-kafka-saga-outbox-and-resilience4j)
12. [Manual Testing & Failure Simulation Guide](#12-manual-testing--failure-simulation-guide)

---

## 1. Project Overview & Architecture

PayFlow Phase 1 implements a synchronous, distributed-ready e-commerce checkout flow with two decoupled microservices:

```
+-------------------------------------------------------------+
|                        Client / User                        |
+-------------------------------------------------------------+
                               |
                               | 1. POST /api/orders
                               v
+-------------------------------------------------------------+
|                        ORDER SERVICE                        |
|                         (Port: 8081)                        |
|                                                             |
|  - Validates request (customerId, amount)                   |
|  - Inserts Order with status = PAYMENT_PENDING              |
|  - Calls Payment Service synchronously via HTTP REST        |
|  - Updates Order to PAID or PAYMENT_FAILED                  |
+-------------------------------------------------------------+
          |                                        |
          | Read/Write                             | 2. POST /api/payments
          v                                        |    (orderId, amount)
+--------------------+                             v
|      order_db      |            +------------------------------------+
|    (PostgreSQL)    |            |          PAYMENT SERVICE           |
|                    |            |            (Port: 8082)            |
|  - orders table    |            |                                    |
+--------------------+            |  - Receives payment request        |
                                  |  - Simulates payment transaction   |
                                  |  - Inserts Payment record (SUCCESS)|
                                  |  - Returns Payment response        |
                                  +------------------------------------+
                                                     |
                                                     | Read/Write
                                                     v
                                          +--------------------+
                                          |     payment_db     |
                                          |    (PostgreSQL)    |
                                          |                    |
                                          |  - payments table  |
                                          +--------------------+
```

---

## 2. Why We Have Two Services

In real-world enterprise architectures, **Orders** and **Payments** represent completely distinct business domains:

1. **Domain Isolation & Single Responsibility:**
   - The **Order Domain** is concerned with shopping carts, product catalog bindings, customer identities, inventory reservation, and fulfillment status.
   - The **Payment Domain** is concerned with financial ledgers, payment gateway integrations (Stripe, Adyen, PayPal), currency conversion, PCI-DSS compliance, refunds, and fraud detection.

2. **Independent Scalability:**
   - Order creation frequency might peak during flash sales, whereas payment verification or external gateway retries might require different computational or throughput characteristics.
   - Decoupling allows deploying and scaling each service independently.

3. **Security and Compliance (PCI-DSS):**
   - Payment systems often handle sensitive financial data governed by strict regulatory compliance (PCI-DSS). Isolating payment logic ensures that security audits and restricted access zones are confined to the `payment-service` rather than expanding to the entire shopping platform.

---

## 3. Why Each Service Has Its Own Database (Database-per-Service)

A foundational rule of microservices is the **Database-per-Service Pattern**:
- `order-service` connects exclusively to `order_db`.
- `payment-service` connects exclusively to `payment_db`.

### Why Not a Shared Database?
If both services connected to a single `shared_db`:
- **Tight Schema Coupling:** If the payment team modifies a column name or table schema in the database, `order-service` could instantly break in production.
- **Bypassed Business Logic:** A developer might be tempted to query or update payment records directly from order queries via SQL `JOIN`s, bypassing payment domain validation, audit logging, and authorization rules.
- **Resource Contention:** A heavy reporting query on orders could lock database tables and stall critical real-time payment transactions.

---

## 4. Why Order Service Must Not Access `payment_db` Directly

Even with two databases on the same physical PostgreSQL instance, `order-service` is strictly forbidden from maintaining a connection to `payment_db`.

### Core Reasons:
1. **Encapsulation:** The internal storage format of payments is a private implementation detail of `payment-service`. The only public contract is the REST API (`POST /api/payments`).
2. **Autonomous Deployment:** `payment-service` must be able to switch its database technology (e.g., from PostgreSQL to CockroachDB or Cassandra) without requiring changes to `order-service`.
3. **Audit and Consistency:** Direct DB writes bypass lifecycle hooks, event emissions, and validation rules maintained by `payment-service`.

---

## 5. Why REST / HTTP Is Used Initially

In Phase 1, we deliberately choose **synchronous REST over HTTP**:
- **Simplicity & Intuitiveness:** Request/response over HTTP is straightforward to write, understand, and debug with standard tools (`curl`, Postman, IDE debuggers).
- **Baseline for Comparison:** By starting with synchronous HTTP, we directly experience its architectural bottlenecks (tight coupling, blocking I/O, cascading outages, and partial failure states) before introducing asynchronous messaging (Kafka) and distributed transaction orchestrators (Sagas).

---

## 6. What Synchronous Communication Means

When `order-service` makes a REST call to `payment-service`:
1. The incoming HTTP request to `order-service` allocates a worker thread (e.g., in Tomcat's thread pool).
2. The thread inserts the order in `order_db` and then opens a socket connection to `http://localhost:8082/api/payments`.
3. **The thread blocks** (sleeps/waits) waiting for `payment-service` to process the payment and send back the HTTP response.
4. While waiting:
   - That thread cannot serve any other incoming user requests.
   - Network latency between the services directly adds to the client's perceived response time.
   - If `payment-service` is slow (e.g., taking 5 seconds per request), `order-service`'s thread pool will quickly exhaust, causing `order-service` to crash or reject new orders entirely (**Cascading Failure**).

---

## 7. What `@Transactional` Guarantees (Single-Database ACID)

Spring's `@Transactional` annotation manages database transactions on a **single `DataSource` / single database connection**:

- **Atomicity:** All SQL statements executed within the method either commit together or roll back completely if an unchecked (`RuntimeException`) exception occurs.
- **Consistency:** Ensures the database transitions from one valid state to another according to schema constraints (foreign keys, not-null constraints, unique checks).
- **Isolation:** Prevents concurrent transactions from seeing uncommitted or dirty data (dependent on the database isolation level, e.g., Read Committed).
- **Durability:** Once committed, changes are written to persistent storage / write-ahead log (WAL).

---

## 8. What `@Transactional` DOES NOT Guarantee Across Services

> [!CAUTION]
> **The Distributed Transaction Illusion:**
> A common misconception is that putting `@Transactional` on `OrderService.createOrder()` ensures the entire order-and-payment flow is atomic.
>
> **It is NOT.**

`@Transactional` only controls the local database connection to `order_db`. It has **zero awareness or control** over:
1. The remote database connection in `payment-service` (`payment_db`).
2. The network connection between the two services.

If an error occurs after `payment-service` commits its transaction, Spring's `@Transactional` in `order-service` can only roll back `order_db`. It cannot undo, cancel, or roll back the already-committed record in `payment_db`.

---

## 9. Failure Scenario 1: Payment Service Is Unavailable

### Scenario Description
1. User submits an order request: `POST /api/orders`.
2. `order-service` creates the order record in `order_db` with `PAYMENT_PENDING`.
3. `order-service` attempts to call `http://localhost:8082/api/payments`.
4. `payment-service` is down, offline, or experiencing network timeout.

### Resulting Behavior in Phase 1
- `RestClient` throws a `ResourceAccessException` (Connection Refused or Socket Timeout).
- `order-service` catches the exception.
- Depending on the handling logic:
  - If handled: `order-service` marks the local order as `PAYMENT_FAILED` and returns an error response (e.g., HTTP 503 / 502).
  - If unhandled or rolled back: The user receives HTTP 500.
- **Key Observation:** Because communication is synchronous, any downtime in `payment-service` immediately degrades and breaks the order creation process.

---

## 10. Failure Scenario 2: Payment Succeeds but Updating the Order Fails (Dual-Write / Partial Failure)

This is the most critical distributed systems problem in commerce architectures.

### Step-by-Step Failure Sequence:
```
Client                 Order Service (order_db)             Payment Service (payment_db)
  |                          |                                   |
  | 1. POST /api/orders      |                                   |
  |------------------------->|                                   |
  |                          | 2. Save Order [PAYMENT_PENDING]   |
  |                          |    (in order_db)                  |
  |                          |                                   |
  |                          | 3. POST /api/payments             |
  |                          |---------------------------------->|
  |                          |                                   | 4. Save Payment [SUCCESS]
  |                          |                                   |    (COMMITTED in payment_db)
  |                          | 5. HTTP 200 OK (Payment SUCCESS)  |
  |                          |<----------------------------------|
  |                          |                                   
  |                          | 6. CRASH / Network Split / Bug    
  |                          |    (Order Service fails BEFORE    
  |                          |     updating Order to PAID)       
  |                          X                                   
  | 7. HTTP 500 Error        |                                   
  |<-------------------------|                                   
```

### Inconsistent State Analysis:
- **`payment_db` State:** Payment #501 status is `SUCCESS`. Real money was captured!
- **`order_db` State:** Order #101 status is still `PAYMENT_PENDING` (or rolled back / not marked `PAID`).
- **Customer Experience:** The customer's credit card was charged $5,000, but the checkout screen showed an error, and the order was never marked as paid or shipped.
- **Root Cause:** Dual-write across two independent databases without a distributed transaction protocol or compensation mechanism.

---

## 11. Why These Limitations Motivate Kafka, Saga, Outbox, and Resilience4j

Phase 1 intentionally exposes these failure modes. In later phases, we will introduce specialized patterns to solve each exact limitation:

| Phase 1 Limitation | Root Cause | Future Solution (Phase 2+) |
|---|---|---|
| **Thread Blocking & Latency** | Synchronous REST calls hold HTTP threads while waiting for downstream services. | **Asynchronous Messaging (Kafka)**: Decouple producers and consumers via event queues. |
| **Cascading Outages** | If `payment-service` crashes, `order-service` immediately fails all checkouts. | **Resilience4j**: Circuit breakers, rate limiters, retries, and fallbacks. |
| **Inconsistent State (Dual-Write Bug)** | DB write in Service A + DB write in Service B cannot be rolled back atomically. | **Saga Pattern (Choreography/Orchestration)**: Execute compensating transactions (e.g. automatic refund) if a step fails. |
| **Lost Events on Crash** | Saving to local DB and sending a network call can fail midway. | **Transactional Outbox Pattern**: Store the message in the same local DB transaction as the business entity, guaranteed by a message relay. |

---

## 12. Manual Testing & Failure Simulation Guide

### Prerequisites
Both services running:
- `order-service` on `http://localhost:8081`
- `payment-service` on `http://localhost:8082`

---

### Test Case 1: Happy Path (Successful Payment & Order Completion)

**Command:**
```bash
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00
  }'
```

**Expected Response (`201 Created`):**
```json
{
  "id": 1,
  "customerId": 42,
  "amount": 5000.00,
  "status": "PAID",
  "createdAt": "2026-09-02T19:30:00",
  "updatedAt": "2026-09-02T19:30:01"
}
```

**Verification:**
- Query order: `curl http://localhost:8081/api/orders/1` -> Status is `PAID`.
- Query payment: `curl http://localhost:8082/api/payments/order/1` -> Status is `SUCCESS`.

---

### Test Case 2: Simulated Payment Failure (Business Rejection)

**Command:**
```bash
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00,
    "simulatePaymentFailure": true
  }'
```

**Expected Response (`200 OK` or `201 Created` with `PAYMENT_FAILED`):**
```json
{
  "id": 2,
  "customerId": 42,
  "amount": 5000.00,
  "status": "PAYMENT_FAILED",
  "createdAt": "2026-09-02T19:31:00",
  "updatedAt": "2026-09-02T19:31:01"
}
```

**Verification:**
- Query order: `curl http://localhost:8081/api/orders/2` -> Status is `PAYMENT_FAILED`.
- Query payment: `curl http://localhost:8082/api/payments/order/2` -> Status is `FAILED`.

---

### Test Case 3: Downstream Service Unavailable (Payment Service Down)

1. Stop `payment-service` (terminate port 8082).
2. Execute order creation:
```bash
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00
  }'
```

**Expected Response (`503 Service Unavailable`):**
```json
{
  "timestamp": "2026-09-02T19:32:00",
  "status": 503,
  "error": "Service Unavailable",
  "message": "Payment service is currently unavailable. Order #3 marked as PAYMENT_FAILED.",
  "path": "/api/orders"
}
```

---

### Test Case 4: Dual-Write Inconsistency Simulation

Simulate a post-payment server failure in `order-service`:
```bash
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00,
    "simulateOrderUpdateFailure": true
  }'
```

**Expected Response (`500 Internal Server Error`):**
```json
{
  "timestamp": "2026-09-02T19:33:00",
  "status": 500,
  "error": "Internal Server Error",
  "message": "Simulated failure occurred after payment was captured. Order state is inconsistent!",
  "path": "/api/orders"
}
```

**Observe Inconsistent State:**
- In `order_db` (`GET /api/orders/4`): Status is `PAYMENT_PENDING`.
- In `payment_db` (`GET /api/payments/order/4`): Status is `SUCCESS`.
- **Result:** The payment was charged, but the order was never fulfilled! This directly proves the fundamental limitation of synchronous REST without a Saga/compensation mechanism.
