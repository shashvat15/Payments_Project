package com.payflow.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.order.dto.CreateOrderRequest;
import com.payflow.order.dto.OrderResponse;
import com.payflow.order.entity.Order;
import com.payflow.order.entity.OrderStatus;
import com.payflow.order.entity.OutboxEvent;
import com.payflow.order.entity.OutboxStatus;
import com.payflow.order.event.OrderCreatedEvent;
import com.payflow.order.event.PaymentProcessedEvent;
import com.payflow.order.exception.OrderProcessingException;
import com.payflow.order.exception.ResourceNotFoundException;
import com.payflow.order.kafka.OrderEventProducer;
import com.payflow.order.repository.OrderRepository;
import com.payflow.order.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OrderEventProducer orderEventProducer,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Phase 2C Transactional Outbox Pattern:
     * 1. Inserts Order with status PAYMENT_PENDING in order_db.
     * 2. Saves OutboxEvent containing the serialized OrderCreatedEvent payload in order_db.
     * 3. Both writes commit atomically in the SAME local database transaction.
     * 4. Kafka is NOT called within this transaction. OutboxPublisher handles Kafka publication separately.
     * 5. Returns immediately with PAYMENT_PENDING status.
     */
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        log.info("Initiating transactional order creation for customerId: {}, amount: {}, productId: {}, quantity: {}",
                request.getCustomerId(), request.getAmount(), request.getProductId(), request.getQuantity());

        // Step 1: Save Order in order_db with status PAYMENT_PENDING
        Order order = new Order(
                request.getCustomerId(),
                request.getAmount(),
                OrderStatus.PAYMENT_PENDING
        );
        order = orderRepository.save(order);
        log.info("Order created in order_db with ID: {} and status: {}", order.getId(), order.getStatus());

        // Step 2: Build OrderCreatedEvent for the Saga Orchestrator
        String sagaId = java.util.UUID.randomUUID().toString();
        Long productId = request.getProductId() != null ? request.getProductId() : 1001L;
        Integer quantity = request.getQuantity() != null ? request.getQuantity() : 1;

        OrderCreatedEvent event = new OrderCreatedEvent(
                sagaId,
                order.getId(),
                order.getCustomerId(),
                productId,
                quantity,
                order.getAmount(),
                request.getSimulatePaymentFailure(),
                request.getSimulateInventoryFailure()
        );

        // Step 3: Save OutboxEvent inside the SAME local database transaction
        try {
            String payload = objectMapper.writeValueAsString(event);
            OutboxEvent outboxEvent = new OutboxEvent(
                    "OrderCreated",
                    "Order",
                    String.valueOf(order.getId()),
                    payload
            );
            outboxEventRepository.save(outboxEvent);
            log.info("[Outbox] OutboxEvent saved in order_db for order #{} [sagaId: {}] with status NEW",
                    order.getId(), sagaId);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize OrderCreatedEvent to JSON for orderId: {}", order.getId(), e);
            throw new OrderProcessingException("Failed to serialize outbox event payload: " + e.getMessage());
        }

        // Step 4: Return immediate response to client (PAYMENT_PENDING)
        return mapToResponse(order);
    }

    @Transactional
    public Order saveInitialOrder(CreateOrderRequest request) {
        Order order = new Order(
                request.getCustomerId(),
                request.getAmount(),
                OrderStatus.PAYMENT_PENDING
        );
        return orderRepository.save(order);
    }

    /**
     * Phase 2B Saga Command: Confirm the order upon successful payment.
     */
    @Transactional
    public Order confirmOrder(Long orderId) {
        log.info("Confirming order #{} via Saga Orchestrator command", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + orderId));
        order.setStatus(OrderStatus.PAID);
        Order updated = orderRepository.save(order);
        log.info("Order #{} confirmed and updated to PAID", orderId);
        return updated;
    }

    /**
     * Phase 2B Saga Command: Cancel the order upon Saga failure or compensation.
     */
    @Transactional
    public Order cancelOrder(Long orderId, String reason) {
        log.warn("Cancelling order #{} via Saga Orchestrator command. Reason: {}", orderId, reason);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + orderId));
        order.setStatus(OrderStatus.CANCELLED);
        Order updated = orderRepository.save(order);
        log.info("Order #{} cancelled and updated to CANCELLED", orderId);
        return updated;
    }

    /**
     * Handles PaymentProcessedEvent received from Payment Service via Kafka (Phase 2A legacy).
     * Transitions order state to PAID or PAYMENT_FAILED.
     */
    @Transactional
    public Order handlePaymentProcessedEvent(PaymentProcessedEvent event) {
        log.info("Handling PaymentProcessedEvent for orderId: {}, paymentId: {}, status: {}",
                event.getOrderId(), event.getPaymentId(), event.getStatus());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + event.getOrderId()));

        if ("SUCCESS".equalsIgnoreCase(event.getStatus())) {
            log.info("Transitioning order #{} from {} to PAID", order.getId(), order.getStatus());
            order.setStatus(OrderStatus.PAID);
        } else {
            log.warn("Transitioning order #{} from {} to PAYMENT_FAILED", order.getId(), order.getStatus());
            order.setStatus(OrderStatus.PAYMENT_FAILED);
        }

        Order updatedOrder = orderRepository.save(order);
        log.info("Order #{} status successfully updated to: {}", updatedOrder.getId(), updatedOrder.getStatus());
        return updatedOrder;
    }

    @Transactional
    public Order updateOrderStatus(Long orderId, OrderStatus newStatus) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + orderId));
        order.setStatus(newStatus);
        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + id));
        return mapToResponse(order);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomerId(Long customerId) {
        return orderRepository.findByCustomerId(customerId).stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<OutboxEvent> getOutboxEvents(OutboxStatus status) {
        if (status != null) {
            return outboxEventRepository.findByStatusOrderByCreatedAtAsc(status);
        }
        return outboxEventRepository.findAll();
    }

    private OrderResponse mapToResponse(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getAmount(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt()
        );
    }
}
