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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private PaymentClient paymentClient;

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
    @DisplayName("Should successfully create order and mark as PAID when payment succeeds")
    void testCreateOrder_Success() {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"));
        PaymentResponseDto paymentSuccess = new PaymentResponseDto(
                101L, 1L, new BigDecimal("5000.00"), "SUCCESS",
                LocalDateTime.now(), LocalDateTime.now()
        );

        Order paidOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAID);
        paidOrder.setId(1L);

        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder, paidOrder);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(paymentClient.processPayment(any(PaymentRequestDto.class))).thenReturn(paymentSuccess);

        OrderResponse response = orderService.createOrder(request);

        assertNotNull(response);
        assertEquals(1L, response.getId());
        assertEquals(42L, response.getCustomerId());
        assertEquals(OrderStatus.PAID, response.getStatus());

        verify(orderRepository, times(2)).save(any(Order.class));
        verify(paymentClient).processPayment(any(PaymentRequestDto.class));
    }

    @Test
    @DisplayName("Should mark order as PAYMENT_FAILED when payment is rejected")
    void testCreateOrder_PaymentFailed() {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"), true, false);
        PaymentResponseDto paymentFailed = new PaymentResponseDto(
                102L, 1L, new BigDecimal("5000.00"), "FAILED",
                LocalDateTime.now(), LocalDateTime.now()
        );

        Order failedOrder = new Order(42L, new BigDecimal("5000.00"), OrderStatus.PAYMENT_FAILED);
        failedOrder.setId(1L);

        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder, failedOrder);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(paymentClient.processPayment(any(PaymentRequestDto.class))).thenReturn(paymentFailed);

        OrderResponse response = orderService.createOrder(request);

        assertNotNull(response);
        assertEquals(OrderStatus.PAYMENT_FAILED, response.getStatus());
        verify(paymentClient).processPayment(any(PaymentRequestDto.class));
    }

    @Test
    @DisplayName("Should mark order as PAYMENT_FAILED and throw ServiceCommunicationException when Payment Service is down")
    void testCreateOrder_ServiceUnavailable() {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"));

        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(pendingOrder));
        when(paymentClient.processPayment(any(PaymentRequestDto.class)))
                .thenThrow(new ServiceCommunicationException("Connection refused"));

        assertThrows(ServiceCommunicationException.class, () -> orderService.createOrder(request));
        assertEquals(OrderStatus.PAYMENT_FAILED, pendingOrder.getStatus());
    }

    @Test
    @DisplayName("Should throw OrderProcessingException when simulated order update failure occurs after payment succeeds")
    void testCreateOrder_SimulateOrderUpdateFailure() {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"), false, true);
        PaymentResponseDto paymentSuccess = new PaymentResponseDto(
                103L, 1L, new BigDecimal("5000.00"), "SUCCESS",
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(paymentClient.processPayment(any(PaymentRequestDto.class))).thenReturn(paymentSuccess);

        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));
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
    @DisplayName("Should get orders by customer ID")
    void testGetOrdersByCustomerId() {
        when(orderRepository.findByCustomerId(42L)).thenReturn(List.of(pendingOrder));

        List<OrderResponse> responses = orderService.getOrdersByCustomerId(42L);

        assertEquals(1, responses.size());
        assertEquals(42L, responses.get(0).getCustomerId());
    }
}
