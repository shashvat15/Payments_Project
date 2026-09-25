# PayFlow Phase 2C — Transactional Outbox Pattern

## 1. Executive Summary

In **Phase 2B**, we introduced the Saga Pattern using Orchestration across `order-service`, `payment-service`, `inventory-service`, and `saga-orchestrator`. While the business saga and compensation flows succeeded, a critical distributed systems flaw remained: **The Dual-Write Problem**.

**Phase 2C** eliminates this dual-write problem by introducing the **Transactional Outbox Pattern** across all stateful domain services:
- **Order Service** (`order_db.outbox_events`)
- **Payment Service** (`payment_db.outbox_events`)
- **Inventory Service** (`inventory_db.outbox_events`)

By storing both the business entity mutation and the event/result payload within the **same local ACID database transaction**, we guarantee that local state changes are never committed without their corresponding outbox events being persisted. Dedicated, asynchronous **Outbox Publishers** poll their respective database outbox tables and publish messages to Apache Kafka with automatic retry on failure.

---

## 2. Distributed Dual-Write Problem in Phase 2B

A distributed service often needs to perform two actions as part of a single business operation:
1. **Local database state change**
2. **Kafka event publication**

In Phase 2B, these were executed as two independent, uncoordinated operations:

```
[Service Business Logic]
        │
        ├─────► 1. Local DB Write (e.g., payment_db: Payment = SUCCESS)
        │
        └─────► 2. Kafka Publication (e.g., Kafka: PaymentProcessedEvent)
```

### Critical Failure Modes:
1. **Database Commit Succeeds, Kafka Publication Fails:**
   - If the application crashes, a network partition occurs, or the Kafka broker is down immediately after the database commit, the database reflects the new state, but Kafka never receives the event.
   - *Example:* In Payment Service, `payment_db` records `Payment = SUCCESS`. However, Kafka fails to deliver `PaymentProcessedEvent`. The Saga Orchestrator never learns that payment succeeded, hanging indefinitely or timing out.
2. **Kafka Publication Succeeds, Database Commit Fails (if Kafka call inside @Transactional):**
   - If message publishing happens inside the transaction and the subsequent database commit fails (e.g., constraint violation, disk full), Kafka receives an event for an entity that was never persisted.
   - *Result:* Phantom downstream processing and corrupted distributed state.

Because distributed 2-Phase Commit (2PC / XA) is slow, blocking, and not supported by Kafka, we cannot wrap a database transaction and a Kafka producer in a single distributed transaction.

---

## 3. Core Design Rule: Data Ownership & Database-per-Service

### The Rule:
> **DO NOT put one central Outbox table in one service or database.**
> 
> Each service owns its own database. When a service changes its local state and needs to reliably publish a message as a consequence of that change, its Outbox belongs in **THAT SERVICE'S OWN DATABASE**.

```
    Service owns database
              +
    Service changes local state
              +
    Service needs to publish resulting message
              =
    Outbox in that service's database
```

### Strict Database-per-Service Isolation:

```
┌────────────────────────┐      ┌────────────────────────┐      ┌────────────────────────┐
│     Order Service      │      │    Payment Service     │      │   Inventory Service    │
│                        │      │                        │      │                        │
│        order_db        │      │       payment_db       │      │      inventory_db      │
│   ┌────────────────┐   │      │   ┌────────────────┐   │      │   ┌────────────────┐   │
│   │ orders         │   │      │   │ payments       │   │      │   │ inventory      │   │
│   │ outbox_events  │   │      │   │ outbox_events  │   │      │   │ outbox_events  │   │
│   └───────┬────────┘   │      │   └───────┬────────┘   │      │   └───────┬────────┘   │
└───────────┼────────────┘      └───────────┼────────────┘      └───────────┼────────────┘
            │                               │                               │
            ▼                               ▼                               ▼
    Order Outbox Publisher         Payment Outbox Publisher        Inventory Outbox Publisher
            │                               │                               │
            ▼                               ▼                               ▼
    Kafka: order-created            Kafka: payment-result           Kafka: inventory-result
```

No service may directly access another service's database. Strict boundaries are maintained:
- `order-service` -> `order_db.outbox_events`
- `payment-service` -> `payment_db.outbox_events`
- `inventory-service` -> `inventory_db.outbox_events`

---

## 4. Producer-Side Dual-Write Boundaries: Service Analysis

### A. Order Service (Outbox Implemented)
- **Database:** `order_db`
- **State Change:** Inserts order with status `PAYMENT_PENDING`.
- **Outgoing Message:** `OrderCreatedEvent` to Kafka topic `order-created`.
- **Boundary:** Client HTTP POST initiates state change. If Kafka fails, no incoming event exists to re-trigger the publish. Without an outbox, the order is permanently orphaned.
- **Solution:** Save `Order` and `OutboxEvent` in `order_db` in the same `@Transactional` method.

### B. Payment Service (Outbox Implemented)
- **Database:** `payment_db`
- **State Change:** Persists `Payment` record with status `SUCCESS` or `FAILED`.
- **Outgoing Message:** `PaymentProcessedEvent` to Kafka topic `payment-result`.
- **Boundary:** Processing `ProcessPaymentCommand` commits payment outcome to `payment_db`. If Kafka publication fails, `payment_db` holds `Payment = SUCCESS`, but Saga Orchestrator never learns the payment succeeded.
- **Solution:** Save `Payment` and `OutboxEvent` in `payment_db` in the same `@Transactional` method.

### C. Inventory Service (Outbox Implemented)
- **Database:** `inventory_db`
- **State Change:** Updates `availableQuantity` and `reservedQuantity` on `InventoryItem` (or releases inventory during compensation).
- **Outgoing Message:** `InventoryResultEvent` (`RESERVED`, `RESERVATION_FAILED`, or `RELEASED`) to Kafka topic `inventory-result`.
- **Boundary:** Processing `InventoryCommand` commits stock mutations to `inventory_db`. If Kafka publication fails, stock is reserved or released in `inventory_db`, but Saga Orchestrator never learns the outcome.
- **Solution:** Save `InventoryItem` and `OutboxEvent` in `inventory_db` in the same `@Transactional` method.

### D. Saga Orchestrator (Outbox NOT Needed)
- **Database:** `saga_db`
- **Role:** Pure event-driven workflow coordinator.
- **Boundary Analysis:** The Saga Orchestrator does not originate primary domain resources. It processes incoming Kafka records inside `@KafkaListener`. If outgoing Kafka command publishing fails, an exception is thrown, rolling back the local transaction and failing consumer offset commit. Kafka's consumer group offset management automatically redelivers the unacknowledged event.
- **Conclusion:** As specified in Section 4 and 9 of the system requirements, an Outbox is not added to Saga Orchestrator to avoid unnecessary complexity and maintain alignment with distributed systems best practices.

---

## 5. Outbox Table Schema

Each service database (`order_db`, `payment_db`, `inventory_db`) contains an identical `outbox_events` table structure:

| Column Name | SQL Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `BIGINT` | `NO` | Primary Key, Auto-increment. |
| `event_type` | `VARCHAR(100)` | `NO` | Event contract identifier (`OrderCreated`, `PaymentProcessed`, `InventoryResult`). |
| `aggregate_type` | `VARCHAR(100)` | `NO` | Aggregate root name (`Order`, `Payment`, `Inventory`). |
| `aggregate_id` | `VARCHAR(100)` | `NO` | ID of the aggregate root. |
| `topic` | `VARCHAR(100)` | `YES` | Target Kafka topic (`order-created`, `payment-result`, `inventory-result`). |
| `message_key` | `VARCHAR(100)` | `YES` | Kafka partitioning key (e.g., `orderId`). |
| `payload` | `TEXT` | `NO` | Serialized JSON representation of the event/result DTO. |
| `status` | `VARCHAR(30)` | `NO` | Lifecycle status: `NEW` or `PUBLISHED`. |
| `created_at` | `TIMESTAMP` | `NO` | Creation timestamp. |
| `published_at` | `TIMESTAMP` | `YES` | Kafka acknowledgment timestamp. |

### Indexes:
- `idx_outbox_status_created` on `(status, created_at)`: Optimizes polling for unpublished events in FIFO chronological order.
- `idx_outbox_aggregate` on `(aggregate_type, aggregate_id)`: Enables fast lookup of outbox records by aggregate.

---

## 6. Same-Transaction Principle

The core invariant of the Transactional Outbox pattern is:

$$\text{Local Business State Change} + \text{Outbox Event Persistence} \in \text{Same Local ACID Transaction}$$

```java
@Transactional
public PaymentProcessedEvent processPaymentFromCommand(ProcessPaymentCommand command) {
    // 1. Mutate local domain entity
    Payment payment = new Payment(command.getOrderId(), command.getAmount(), status);
    Payment savedPayment = paymentRepository.save(payment);

    // 2. Prepare event DTO
    PaymentProcessedEvent event = new PaymentProcessedEvent(...);

    // 3. Persist OutboxEvent in the same transaction
    String payload = objectMapper.writeValueAsString(event);
    OutboxEvent outboxEvent = new OutboxEvent(
            "PaymentProcessed", "Payment", String.valueOf(savedPayment.getId()),
            "payment-result", String.valueOf(savedPayment.getOrderId()), payload
    );
    outboxEventRepository.save(outboxEvent);

    // 4. Return event — Kafka is NOT invoked here!
    return event;
}
```

- If transaction **rolls back**: neither domain entity nor outbox record is committed.
- If transaction **commits**: both domain entity and outbox record are durably persisted.

---

## 7. Outbox Publisher Design & Polling Loop

Each service runs a dedicated, scheduled background publisher (`OutboxPublisher`):

1. **Poll:** Every 2,000ms (`@Scheduled(fixedDelayString = "${app.outbox.publisher.fixed-delay:2000}")`), query `outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW)`.
2. **Deserialize:** Parse the stored JSON payload into the strongly typed Kafka DTO (`PaymentProcessedEvent`, `InventoryResultEvent`, or `OrderCreatedEvent`).
3. **Publish:** Send to Kafka using `kafkaTemplate.send(topic, messageKey, payload)`.
4. **Synchronous Acknowledgment:** Wait up to 5,000ms (`timeout-ms`) via `future.get(timeout, TimeUnit.MILLISECONDS)`.
5. **Success:** Mark status as `PUBLISHED`, record `publishedAt = LocalDateTime.now()`, and save to database.
6. **Failure / Broker Down:** Catch exception, log error, leave record as `NEW`. The record will be picked up on the next polling run.

---

## 8. Kafka Down Scenario Walkthrough

```
Payment Service                  payment_db                 Kafka Broker
      │                              │                           │
      ├──── processPayment() ───────►│                           │
      │     Save Payment = SUCCESS   │                           │
      │     Save Outbox = NEW        │                           │
      │     COMMIT                   │                           │
      │◄─────────────────────────────┤                           │
      │                              │                           │
  [Outbox Publisher Poll]            │                           │
      ├──── Find NEW outbox ────────►│                           │
      │◄─── OutboxEvent [NEW] ───────┤                           │
      │                                                          │
      ├──── Publish PaymentProcessedEvent ──────────────────────►│  [KAFKA DOWN!]
      │                                                          X  (Connection refused)
      │◄─── Exception: Timeout / Unreachable ────────────────────┤
      │                                                          │
      ├──── Keep Status = NEW ──────►│                           │
      │                              │                           │
      ... Time passes ... Kafka restored ...                     │
      │                                                          │
  [Next Publisher Poll]                                          │
      ├──── Find NEW outbox ────────►│                           │
      │◄─── OutboxEvent [NEW] ───────┤                           │
      │                                                          │
      ├──── Publish PaymentProcessedEvent ──────────────────────►│  [KAFKA ONLINE]
      │◄─── Acknowledged (partition 0, offset 12) ───────────────┤  ACK Received!
      │                                                          │
      ├──── Update Outbox = PUBLISHED►│                          │
      │    (publishedAt = timestamp) │                          │
```

**Outcome:** The business state change is never lost, and event delivery resumes automatically once the message broker recovers.

---

## 9. Duplicate-Delivery Limitation (At-Least-Once Delivery)

> [!WARNING]
> **Transactional Outbox provides AT-LEAST-ONCE delivery, NOT exactly-once processing.**

Consider this failure sequence:
1. `OutboxPublisher` sends `PaymentProcessedEvent` to Kafka.
2. Kafka receives and persists the message.
3. The publisher process crashes, encounters a network timeout, or loses database connectivity **BEFORE** it can update the outbox record to `PUBLISHED`.
4. The outbox record remains `NEW`.
5. Upon restart or on the next poll cycle, `OutboxPublisher` retrieves the event again and re-publishes it to Kafka.
6. Downstream consumers receive a **duplicate message**.

### Phase Scope Confirmation:
- **Phase 2C** solves the dual-write problem (guarantees zero message loss).
- Idempotent consumers, idempotency keys, and Redis state tracking belong to **Phase 2D**.
- Redis and idempotent consumers were **NOT** implemented in Phase 2C.

---

## 10. Verification & Test Suite

### Full Maven Test Results:
- **Order Service:** 20 tests (100% passing)
- **Payment Service:** 16 tests (100% passing)
- **Inventory Service:** 15 tests (100% passing)
- **Saga Orchestrator:** 6 tests (100% passing)
- **Total:** 57 tests passing across the multi-module reactor with 0 failures and 0 errors.

### Observability Endpoints:
Each service exposes an endpoint to inspect its outbox table:
- Order Service: `GET http://localhost:8081/api/orders/outbox`
- Payment Service: `GET http://localhost:8082/api/payments/outbox`
- Inventory Service: `GET http://localhost:8083/api/inventory/outbox`
