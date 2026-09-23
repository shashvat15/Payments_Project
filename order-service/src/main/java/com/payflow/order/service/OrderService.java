package com.payflow.order.service;

import com.payflow.order.dto.CreateOrderRequest;
import com.payflow.order.dto.OrderResponse;
import com.payflow.order.entity.Order;
import com.payflow.order.entity.OrderStatus;
import com.payflow.order.event.OrderCreatedEvent;
import com.payflow.order.event.PaymentProcessedEvent;
import com.payflow.order.exception.OrderProcessingException;
import com.payflow.order.exception.ResourceNotFoundException;
import com.payflow.order.kafka.OrderEventProducer;
import com.payflow.order.repository.OrderRepository;
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

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    /**
     * Phase 2A Asynchronous Checkout Flow:
     * 1. Inserts Order with status PAYMENT_PENDING in order_db.
     * 2. Publishes OrderCreatedEvent to Kafka topic 'order-created'.
     * 3. Returns immediately with PAYMENT_PENDING status (non-blocking).
     */
    public OrderResponse createOrder(CreateOrderRequest request) {
        log.info("Initiating asynchronous order creation for customerId: {}, amount: {}",
                request.getCustomerId(), request.getAmount());

        // Step 1: Save Order in order_db with status PAYMENT_PENDING
        Order order = saveInitialOrder(request);
        log.info("Order created in order_db with ID: {} and status: {}", order.getId(), order.getStatus());

        // Step 2: Simulate the Dual-Write Failure Problem (Order DB write succeeds, but Kafka publish fails)
        if (Boolean.TRUE.equals(request.getSimulateKafkaPublishFailure())) {
            log.error("SIMULATION: Intentionally crashing during Kafka publish for order ID {} to demonstrate Dual-Write inconsistency!", order.getId());
            throw new OrderProcessingException(
                    "Simulated Dual-Write failure: Order #" + order.getId()
                            + " was committed to order_db, but Kafka event publishing failed! Order is permanently stuck in PAYMENT_PENDING.");
        }

        // Step 3: Publish OrderCreatedEvent to Kafka asynchronously
        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(),
                order.getCustomerId(),
                order.getAmount(),
                request.getSimulatePaymentFailure()
        );

        orderEventProducer.sendOrderCreatedEvent(event);
        log.info("OrderCreatedEvent sent to Kafka for orderId: {}", order.getId());

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
     * Handles PaymentProcessedEvent received from Payment Service via Kafka.
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
