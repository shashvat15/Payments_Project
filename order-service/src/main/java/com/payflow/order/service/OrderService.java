package com.payflow.order.service;

import com.payflow.order.client.PaymentClient;
import com.payflow.order.dto.CreateOrderRequest;
import com.payflow.order.dto.OrderResponse;
import com.payflow.order.dto.PaymentRequestDto;
import com.payflow.order.dto.PaymentResponseDto;
import com.payflow.order.entity.Order;
import com.payflow.order.entity.OrderStatus;
import com.payflow.order.exception.OrderProcessingException;
import com.payflow.order.exception.ResourceNotFoundException;
import com.payflow.order.exception.ServiceCommunicationException;
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
    private final PaymentClient paymentClient;

    public OrderService(OrderRepository orderRepository, PaymentClient paymentClient) {
        this.orderRepository = orderRepository;
        this.paymentClient = paymentClient;
    }

    /**
     * Executes synchronous checkout flow:
     * 1. Inserts Order with PAYMENT_PENDING into order_db
     * 2. Synchronously invokes Payment Service via REST
     * 3. Updates Order status to PAID or PAYMENT_FAILED
     * 4. Returns final OrderResponse
     */
    public OrderResponse createOrder(CreateOrderRequest request) {
        log.info("Initiating order creation for customerId: {}, amount: {}",
                request.getCustomerId(), request.getAmount());

        // Step 1: Create and persist order with PAYMENT_PENDING in order_db
        Order order = saveInitialOrder(request);
        log.info("Order created with ID: {} and status: {}", order.getId(), order.getStatus());

        // Step 2: Synchronous REST call to Payment Service
        PaymentRequestDto paymentRequest = new PaymentRequestDto(
                order.getId(),
                order.getAmount(),
                request.getSimulatePaymentFailure()
        );

        PaymentResponseDto paymentResponse;
        try {
            paymentResponse = paymentClient.processPayment(paymentRequest);
        } catch (ServiceCommunicationException ex) {
            log.error("Payment Service unavailable for Order ID {}. Marking order as PAYMENT_FAILED", order.getId());
            updateOrderStatus(order.getId(), OrderStatus.PAYMENT_FAILED);
            throw ex;
        }

        // Step 3: Handle Payment outcome
        if (paymentResponse != null && "SUCCESS".equalsIgnoreCase(paymentResponse.getStatus())) {
            log.info("Payment succeeded for Order ID: {}", order.getId());

            // Simulation hook for Dual-Write Consistency Failure
            if (Boolean.TRUE.equals(request.getSimulateOrderUpdateFailure())) {
                log.error("SIMULATION: Intentionally crashing AFTER payment success to demonstrate distributed inconsistency!");
                throw new OrderProcessingException(
                        "Simulated failure: Payment # " + paymentResponse.getId()
                                + " succeeded in payment_db, but order-service crashed before updating order #"
                                + order.getId() + " to PAID. Order remains PAYMENT_PENDING in order_db!");
            }

            Order updatedOrder = updateOrderStatus(order.getId(), OrderStatus.PAID);
            return mapToResponse(updatedOrder);
        } else {
            log.warn("Payment failed or rejected for Order ID: {}", order.getId());
            Order updatedOrder = updateOrderStatus(order.getId(), OrderStatus.PAYMENT_FAILED);
            return mapToResponse(updatedOrder);
        }
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
