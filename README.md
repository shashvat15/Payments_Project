# PayFlow — Distributed Payment System

PayFlow is a backend learning project designed to teach and demonstrate distributed systems concepts step-by-step.

---

## Current Status: Phase 1 (Basic Synchronous Backend)

In Phase 1, we implement a synchronous 2-service architecture to understand why distributed patterns (Kafka, Saga, Outbox, Resilience4j) are needed.

Detailed conceptual documentation: 👉 [`docs/phase-1.md`](docs/phase-1.md)

---

## System Architecture

```
Client
  |
  | 1. POST /api/orders
  v
+-----------------------+                    +-------------------------+
|     order-service     | --- HTTP REST ---> |     payment-service     |
|      (Port: 8081)     |                    |       (Port: 8082)      |
+-----------------------+                    +-------------------------+
            |                                             |
            v                                             v
+-----------------------+                    +-------------------------+
|        order_db       |                    |        payment_db       |
|      (PostgreSQL)     |                    |       (PostgreSQL)      |
+-----------------------+                    +-------------------------+
```

### Key Architectural Constraints
1. **Database-per-Service**: `order-service` connects strictly to `order_db`; `payment-service` connects strictly to `payment_db`.
2. **Synchronous REST**: Services communicate over HTTP.
3. **No Distributed Transactions**: Demonstrates dual-write inconsistency and partial failure problems.

---

## Quick Start with Docker

To build and run both databases and both services simultaneously:

```bash
docker-compose up --build
```

Services will be accessible at:
- **Order Service**: `http://localhost:8081`
- **Payment Service**: `http://localhost:8082`
- **Order DB (PostgreSQL)**: `localhost:5432` (`order_db`)
- **Payment DB (PostgreSQL)**: `localhost:5433` (`payment_db`)

---

## Running Locally with Maven

### 1. Start PostgreSQL Databases
Create two databases:
```sql
CREATE DATABASE order_db;
CREATE DATABASE payment_db;
```

### 2. Run Payment Service
```bash
cd payment-service
mvn spring-boot:run
```

### 3. Run Order Service
```bash
cd order-service
mvn spring-boot:run
```

---

## API Testing & Failure Simulation

### 1. Create Order (Happy Path - Success)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00
  }'
```
Response:
```json
{
  "id": 1,
  "customerId": 42,
  "amount": 5000.00,
  "status": "PAID"
}
```

### 2. Simulate Payment Failure (Business Rejection)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00,
    "simulatePaymentFailure": true
  }'
```
Response:
```json
{
  "id": 2,
  "customerId": 42,
  "amount": 5000.00,
  "status": "PAYMENT_FAILED"
}
```

### 3. Simulate Downstream Service Down
Stop `payment-service` and send an order creation request.
`order-service` returns `503 Service Unavailable` with clean error details.

### 4. Simulate Dual-Write Consistency Failure (The Core Distributed Problem)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 42,
    "amount": 5000.00,
    "simulateOrderUpdateFailure": true
  }'
```
- Payment #3 is marked `SUCCESS` in `payment_db`.
- Order #3 is left in `PAYMENT_PENDING` in `order_db`.
- User gets `500 Internal Server Error`.
- **Learning Observation:** Money was captured, but the order was never marked as paid. This demonstrates the fundamental problem that Phase 2 will solve using the **Saga Pattern** and **Transactional Outbox**.
