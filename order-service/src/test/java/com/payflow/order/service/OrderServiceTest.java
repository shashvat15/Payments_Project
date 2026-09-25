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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.payflow.order.entity.OutboxEvent;
import com.payflow.order.entity.OutboxStatus;
import com.payflow.order.repository.OutboxEventRepository;
import org.mockito.Spy;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventProducer orderEventProducer;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @InjectMocks
    private OrderService orderService;

    private Order pendingOrder;

    @BeforeEach
    void setUp() {
        pendingOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAYMENT_PENDING);
        pendingOrder.setId(1L);
        pendingOrder.setCreatedAt(LocalDateTime.now());
        pendingOrder.setUpdatedAt(LocalDateTime.now());
    }

    @Test
    @DisplayName("Should create order and save OutboxEvent in local DB transaction without calling Kafka directly")
    void testCreateOrder_Success() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"));
        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);

        OrderResponse response = orderService.createOrder(request);

        assertNotNull(response);
        assertEquals(1L, response.getId());
        assertEquals(42L, response.getCustomerId());
        assertEquals(OrderStatus.PAYMENT_PENDING, response.getStatus());

        // Verify Order is saved in order_db
        verify(orderRepository).save(any(Order.class));

        // Verify OutboxEvent is saved in order_db with status NEW
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        OutboxEvent savedOutbox = outboxCaptor.getValue();
        assertEquals("OrderCreated", savedOutbox.getEventType());
        assertEquals("Order", savedOutbox.getAggregateType());
        assertEquals("1", savedOutbox.getAggregateId());
        assertEquals(OutboxStatus.NEW, savedOutbox.getStatus());
        assertNotNull(savedOutbox.getPayload());

        // Verify Kafka is NOT directly called from within the business transaction
        verify(orderEventProducer, never()).sendOrderCreatedEvent(any());

        // Verify Outbox payload deserializes to OrderCreatedEvent
        OrderCreatedEvent deserialized = objectMapper.readValue(savedOutbox.getPayload(), OrderCreatedEvent.class);
        assertEquals(1L, deserialized.getOrderId());
        assertEquals(42L, deserialized.getCustomerId());
        assertEquals(new BigDecimal("5000.00"), deserialized.getAmount());
        assertNotNull(deserialized.getSagaId());
    }

    @Test
    @DisplayName("Should persist Order and OutboxEvent atomically as part of the same transaction")
    void testCreateOrder_OrderAndOutboxSavedTogether() {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"), false, false, false);
        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);

        OrderResponse response = orderService.createOrder(request);

        assertNotNull(response);
        verify(orderRepository, times(1)).save(any(Order.class));
        verify(outboxEventRepository, times(1)).save(any(OutboxEvent.class));
        verify(orderEventProducer, never()).sendOrderCreatedEvent(any());
    }

    @Test
    @DisplayName("Should transition order to PAID when PaymentProcessedEvent status is SUCCESS")
    void testHandlePaymentProcessedEvent_Success() {
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "evt-123", 1L, 101L, new BigDecimal("5000.00"), "SUCCESS", LocalDateTime.now()
        );

        Order paidOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAID);
        paidOrder.setId(1L);

        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenReturn(paidOrder);

        Order result = orderService.handlePaymentProcessedEvent(event);

        assertEquals(OrderStatus.PAID, result.getStatus());
        verify(orderRepository).save(pendingOrder);
    }

    @Test
    @DisplayName("Should transition order to PAYMENT_FAILED when PaymentProcessedEvent status is FAILED")
    void testHandlePaymentProcessedEvent_Failed() {
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "evt-124", 1L, 102L, new BigDecimal("5000.00"), "FAILED", LocalDateTime.now()
        );

        Order failedOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAYMENT_FAILED);
        failedOrder.setId(1L);

        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenReturn(failedOrder);

        Order result = orderService.handlePaymentProcessedEvent(event);

        assertEquals(OrderStatus.PAYMENT_FAILED, result.getStatus());
    }

    @Test
    @DisplayName("Should get order by ID")
    void testGetOrderById_Found() {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));

        OrderResponse response = orderService.getOrderById(1L);

        assertNotNull(response);
        assertEquals(1L, response.getId());
    }

    @Test
    @DisplayName("Should throw ResourceNotFoundException when order ID is not found")
    void testGetOrderById_NotFound() {
        when(orderRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> orderService.getOrderById(999L));
    }

    @Test
    @DisplayName("Should confirm order and set status to PAID on Saga ConfirmOrder command")
    void testConfirmOrder_Success() {
        Order paidOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAID);
        paidOrder.setId(1L);

        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenReturn(paidOrder);

        Order result = orderService.confirmOrder(1L);

        assertEquals(OrderStatus.PAID, result.getStatus());
        verify(orderRepository).save(pendingOrder);
    }

    @Test
    @DisplayName("Should cancel order and set status to CANCELLED on Saga CancelOrder command")
    void testCancelOrder_Success() {
        Order cancelledOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.CANCELLED);
        cancelledOrder.setId(1L);

        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenReturn(cancelledOrder);

        Order result = orderService.cancelOrder(1L, "Payment failed");

        assertEquals(OrderStatus.CANCELLED, result.getStatus());
        verify(orderRepository).save(pendingOrder);
    }

    @Test
    @DisplayName("Should get orders by customer ID")
    void testGetOrdersByCustomerId() {
        when(orderRepository.findByCustomerId(42L)).thenReturn(List.of(pendingOrder));

        List<OrderResponse> responses = orderService.getOrdersByCustomerId(42L);

        assertEquals(1, responses.size());
        assertEquals(42L, responses.get(0).getCustomerId());
    }
}
