# PayFlow — Distributed Payment System

PayFlow is a backend learning project designed to teach and demonstrate distributed systems concepts step-by-step.

---

## Current Status: Phase 2A (Kafka & Asynchronous Event-Driven Architecture)

In **Phase 2A**, we replaced synchronous REST communication with **Apache Kafka event streams** to achieve non-blocking execution, high throughput, and temporal decoupling.

- Detailed Phase 1 documentation: 👉 [`docs/phase-1.md`](docs/phase-1.md)
- Detailed Phase 2A documentation: 👉 [`docs/phase-2-kafka.md`](docs/phase-2-kafka.md)

---

## System Architecture (Phase 2A)

```
Client
  |
  | 1. POST /api/orders (Returns immediately: PAYMENT_PENDING)
  v
+-----------------------+                    +-------------------------+
|     order-service     |                    |     payment-service     |
|      (Port: 8081)     |                    |       (Port: 8082)      |
+-----------------------+                    +-------------------------+
       |         |                                      ^         |
       |         | 2. OrderCreatedEvent                 |         |
       v         +------------------> [ Kafka ] --------+         |
  +----------+                        [ Topic ]                   |
  | order_db |                     'order-created'                |
  +----------+                                                    |
       ^                                                          |
       | 4. Update Order (PAID)       [ Kafka ]                   v
       +----------------------------- [ Topic ] <-----------------+
                                 'payment-processed'  3. PaymentProcessedEvent
                                                                  |
                                                                  v
                                                            +------------+
                                                            | payment_db |
                                                            +------------+
```

### Kafka Topics & Consumer Groups
- **`order-created`**: Emitted by `order-service` -> Consumed by `payment-service` (`group-id: payment-service-group`).
- **`payment-processed`**: Emitted by `payment-service` -> Consumed by `order-service` (`group-id: order-service-group`).

---

## Quick Start with Docker Compose

To build and run all databases, Kafka, and both microservices:

```bash
docker-compose up --build
```

### Services & Ports:
- **Order Service**: `http://localhost:8081`
- **Payment Service**: `http://localhost:8082`
- **Apache Kafka (KRaft)**: `localhost:9092`
- **Order DB (PostgreSQL)**: `localhost:5432` (`order_db`)
- **Payment DB (PostgreSQL)**: `localhost:5433` (`payment_db`)

---

## API Testing with Postman / cURL

Import [`payflow.postman_collection.json`](payflow.postman_collection.json) or run cURL:

### 1. Create Order (Asynchronous Happy Path)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": 42, "amount": 5000.00}'
```
- Returns immediately (`~15ms`): Status is `PAYMENT_PENDING`.
- Within `~1-2s`: Query `GET http://localhost:8081/api/orders/1` -> Status is `PAID`.
- Check Payment: Query `GET http://localhost:8082/api/payments/order/1` -> Status is `SUCCESS`.

### 2. Simulate Payment Failure via Kafka
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": 42, "amount": 5000.00, "simulatePaymentFailure": true}'
```
- Query `GET http://localhost:8081/api/orders/2` -> Status is `PAYMENT_FAILED`.

### 3. Observe the Dual-Write Problem (Motivation for Phase 3)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": 42, "amount": 5000.00, "simulateKafkaPublishFailure": true}'
```
- Order is saved in `order_db` as `PAYMENT_PENDING`, but the Kafka publish fails.
- The order is stuck forever with no payment processed, proving why the **Transactional Outbox Pattern** is needed in Phase 3.
