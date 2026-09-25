# PayFlow Phase 2C — Transactional Outbox Pattern

## 1. Executive Summary

In **Phase 2B**, we introduced the Saga Pattern using Orchestration across `order-service`, `payment-service`, `inventory-service`, and `saga-orchestrator`. While the business saga and compensation flows succeeded, a critical distributed systems flaw remained: **The Dual-Write Problem**.

**Phase 2C** eliminates this dual-write problem by introducing the **Transactional Outbox Pattern** in the Order Service. By storing both the business entity (`orders`) and the event payload (`outbox_events`) within the **same local ACID database transaction**, we guarantee that an order is never saved without its corresponding event being persisted. A separate, asynchronous **Outbox Publisher** guarantees at-least-once message delivery to Apache Kafka.

---

## 2. The Problem: Dual-Write Inconsistency in Phase 2B

In Phase 2B, creating an order involved two uncoordinated distributed operations:

```
@Transactional
public OrderResponse createOrder(...) {
    // Operation 1: Local Database Write (order_db)
    Order order = saveInitialOrder(request); // COMMITTED

    // Operation 2: External Network Call (Kafka)
    orderEventProducer.sendOrderCreatedEvent(event); // NETWORK CALL
}
```

### Failure Modes of Dual-Write:
1. **Database Commit Succeeds, Kafka Publish Fails:**
   - If the application crashes, network partition occurs, or Kafka broker is down immediately after the database write, the order remains in `order_db` in `PAYMENT_PENDING` status.
   - However, the `OrderCreatedEvent` is never published.
   - Downstream services (`saga-orchestrator`, `inventory-service`, `payment-service`) never learn that the order was placed.
   - **Result:** The order is permanently orphaned in the database.
2. **Kafka Publish Succeeds, Database Commit Fails (if Kafka call inside @Transactional):**
   - If message publishing happens inside the transaction and the subsequent database commit fails (e.g., constraint violation, disk full), Kafka receives an event for an order that never existed.
   - **Result:** Phantom downstream processing and financial inconsistencies.

Because distributed 2-Phase Commit (2PC / XA) is slow, blocking, and not supported by Kafka, we cannot simply wrap a database transaction and a Kafka producer in a single distributed transaction.

---

## 3. The Solution: Transactional Outbox Pattern

The **Transactional Outbox Pattern** converts the network call into a local database write:

```
                        Order Service
                             │
                             ▼
               ┌───────────────────────────┐
               │    Local DB Transaction   │
               │         (order_db)        │
               │                           │
               │ 1. INSERT INTO orders     │
               │ 2. INSERT INTO outbox     │
               └─────────────┬─────────────┘
                             │
                           COMMIT
                             │
                             ▼
                    Outbox Publisher
               (Periodic Polling Component)
                             │
                             ▼
                   Kafka: order-created
                             │
                             ▼
                     Saga Orchestrator
```

### Core Mechanism:
1. When a client places an order (`POST /api/orders`), the Order Service opens a local database transaction.
2. It inserts the `Order` record into the `orders` table.
3. It constructs the `OrderCreatedEvent`, serializes it to JSON, and inserts an `OutboxEvent` record into the `outbox_events` table with status `NEW`.
4. The transaction **COMMITS** both records atomically to `order_db`.
5. An asynchronous scheduled background process (`OutboxPublisher`) periodically scans for `NEW` events, publishes them to Kafka, and upon receiving confirmation from the broker, marks the event as `PUBLISHED`.

---

## 4. Architecture & Database Design

### Outbox Table Schema (`outbox_events` in `order_db`)

| Column Name | SQL Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `BIGINT` | `NO` | Primary Key, Auto-increment. |
| `event_type` | `VARCHAR(100)` | `NO` | Identifies the event contract (`OrderCreated`). |
| `aggregate_type` | `VARCHAR(100)` | `NO` | The business aggregate root (`Order`). |
| `aggregate_id` | `VARCHAR(100)` | `NO` | The ID of the aggregate (`order.getId()`). |
| `payload` | `TEXT` | `NO` | Serialized JSON representation of `OrderCreatedEvent`. |
| `status` | `VARCHAR(30)` | `NO` | Lifecycle state: `NEW` or `PUBLISHED`. |
| `created_at` | `TIMESTAMP` | `NO` | Timestamp when outbox record was created. |
| `published_at` | `TIMESTAMP` | `YES` | Timestamp when Kafka acknowledged receipt. |

### Indexes:
- `idx_outbox_status_created` on `(status, created_at)`: Optimizes polling for unpublished events in chronological order.
- `idx_outbox_aggregate` on `(aggregate_type, aggregate_id)`: Enables fast lookup of outbox records for a specific order.

---

## 5. Execution Flow

### Step 1: Atomic Database Write
```java
@Transactional
public OrderResponse createOrder(CreateOrderRequest request) {
    // 1. Save Order entity
    Order order = new Order(request.getCustomerId(), request.getAmount(), OrderStatus.PAYMENT_PENDING);
    order = orderRepository.save(order);

    // 2. Build domain event
    String sagaId = UUID.randomUUID().toString();
    OrderCreatedEvent event = new OrderCreatedEvent(
            sagaId, order.getId(), order.getCustomerId(), productId, quantity, order.getAmount(),
            request.getSimulatePaymentFailure(), request.getSimulateInventoryFailure()
    );

    // 3. Save Outbox event
    String payload = objectMapper.writeValueAsString(event);
    OutboxEvent outboxEvent = new OutboxEvent("OrderCreated", "Order", String.valueOf(order.getId()), payload);
    outboxEventRepository.save(outboxEvent);

    return mapToResponse(order);
}
```

### Step 2: Outbox Publisher Polling & Publishing
`OutboxPublisher` runs periodically via Spring's `@Scheduled(fixedDelay = 2000)`:
1. Queries `outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW)`.
2. For each pending event:
   - Deserializes `payload` into `OrderCreatedEvent`.
   - Invokes `orderEventProducer.sendOrderCreatedEvent(eventPayload).get(5, TimeUnit.SECONDS)`.
   - When the Kafka broker acknowledges the message (`RecordMetadata` received), updates `outboxEvent.setStatus(OutboxStatus.PUBLISHED)` and `outboxEvent.setPublishedAt(LocalDateTime.now())`.
   - Saves the updated `OutboxEvent` to `order_db`.
3. If Kafka is unreachable or the call times out:
   - Catches the exception and logs an error: `Publication failed for event id=... Retrying event on next run.`
   - Leaves the status as `NEW`.
   - On the next poll cycle, the publisher picks up the event and retries.

---

## 6. Failure Semantics & Resilience

### What Happens When Kafka is Down?
1. Client submits `POST /api/orders`.
2. `order-service` writes the order and the outbox event to `order_db`. The HTTP response returns `201 CREATED` immediately.
3. `OutboxPublisher` attempts to publish to Kafka.
4. Kafka is unreachable, throwing a connection timeout.
5. The outbox record remains in status `NEW` in `order_db`.
6. **No data is lost.**
7. When Kafka recovers, the next scheduled poll by `OutboxPublisher` reads the pending `NEW` event, successfully publishes it, and transitions the status to `PUBLISHED`.
8. The Saga Orchestrator receives the event and proceeds with the saga workflow as normal.

---

## 7. Known Limitation: Duplicate Delivery (At-Least-Once Delivery)

The Transactional Outbox pattern guarantees **at-least-once delivery**, NOT **exactly-once delivery**:

```
           Outbox Publisher
                 │
                 ▼
          1. Publish to Kafka ✓ (Broker ACKed)
                 │
                 ▼
        [CRASH / NETWORK DROP]
                 │
                 ▼
          Before marking outbox as PUBLISHED in order_db ✗
                 │
                 ▼
          Process restarts / next poll
                 │
                 ▼
          Outbox record is still NEW!
                 │
                 ▼
          2. Publisher retries and publishes again!
```

Because publishing to Kafka and updating the outbox status in the database are two separate actions, a crash between step 1 and step 2 causes the same event to be published twice to Kafka.

### Why Idempotency is Deferred to Phase 2D:
- Solving duplicate messages requires **Idempotent Consumers** (using a distributed lock or deduplication store such as Redis).
- Phase 2C intentionally focuses solely on the **producer-side reliability (Outbox)**.
- Consumer-side deduplication and idempotency are the explicit focus of **Phase 2D**.

---

## 8. Manual Verification Procedure

### Procedure: Testing Resilience to Kafka Outages

#### Step 1: Stop Kafka
```bash
docker compose stop kafka
```

#### Step 2: Create an Order
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": 42, "amount": 5000.00, "productId": 1001, "quantity": 1}'
```
**Expected Response:**
- HTTP `201 Created`
- Body: `{"id": 1, "customerId": 42, "amount": 5000.00, "status": "PAYMENT_PENDING"}`

#### Step 3: Verify Outbox Table in `order_db`
```bash
curl http://localhost:8081/api/orders/outbox?status=NEW
```
*Or via SQL:*
```bash
docker exec -it payflow-order-db psql -U postgres -d order_db -c "SELECT id, aggregate_id, event_type, status, created_at, published_at FROM outbox_events;"
```
**Expected Result:**
- Outbox event exists.
- `status`: `NEW`
- `published_at`: `NULL`
- Order exists in `orders` table.

#### Step 4: Restart Kafka
```bash
docker compose start kafka
```

#### Step 5: Verify Event Published & Saga Completed
Within 2-3 seconds, check the outbox status again:
```bash
curl http://localhost:8081/api/orders/outbox
```
**Expected Result:**
- `status`: `PUBLISHED`
- `published_at`: Populated with timestamp.

Check the order status:
```bash
curl http://localhost:8081/api/orders/1
```
**Expected Result:**
- `status`: `PAID` (Saga completed successfully).
