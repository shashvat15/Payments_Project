package com.payflow.order.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.order.dto.CreateOrderRequest;
import com.payflow.order.dto.OrderResponse;
import com.payflow.order.entity.OrderStatus;
import com.payflow.order.exception.ResourceNotFoundException;
import com.payflow.order.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService orderService;

    @Test
    @DisplayName("POST /api/orders should return 201 with OrderResponse in PAYMENT_PENDING state")
    void testCreateOrder_Success() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(42L, new BigDecimal("5000.00"));
        OrderResponse response = new OrderResponse(
                1L, 42L, new BigDecimal("5000.00"), OrderStatus.PAYMENT_PENDING,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(orderService.createOrder(any(CreateOrderRequest.class))).thenReturn(response);

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.customerId").value(42))
                .andExpect(jsonPath("$.amount").value(5000.00))
                .andExpect(jsonPath("$.status").value("PAYMENT_PENDING"));
    }

    @Test
    @DisplayName("POST /api/orders with invalid data should return 400 Bad Request")
    void testCreateOrder_ValidationFailure() throws Exception {
        CreateOrderRequest invalidRequest = new CreateOrderRequest(null, new BigDecimal("-5.00"));

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidRequest)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.validationErrors.customerId").exists())
                .andExpect(jsonPath("$.validationErrors.amount").exists());
    }

    @Test
    @DisplayName("GET /api/orders/{id} should return 200 when found")
    void testGetOrderById_Found() throws Exception {
        OrderResponse response = new OrderResponse(
                1L, 42L, new BigDecimal("5000.00"), OrderStatus.PAID,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(orderService.getOrderById(1L)).thenReturn(response);

        mockMvc.perform(get("/api/orders/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.status").value("PAID"));
    }

    @Test
    @DisplayName("GET /api/orders/{id} should return 404 when not found")
    void testGetOrderById_NotFound() throws Exception {
        when(orderService.getOrderById(999L))
                .thenThrow(new ResourceNotFoundException("Order not found with id: 999"));

        mockMvc.perform(get("/api/orders/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("Order not found with id: 999"));
    }

    @Test
    @DisplayName("GET /api/orders should return 200 with list of orders")
    void testGetAllOrders() throws Exception {
        OrderResponse response = new OrderResponse(
                1L, 42L, new BigDecimal("5000.00"), OrderStatus.PAID,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(orderService.getAllOrders()).thenReturn(List.of(response));

        mockMvc.perform(get("/api/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(1))
                .andExpect(jsonPath("$[0].status").value("PAID"));
    }

    @Test
    @DisplayName("GET /api/orders/outbox should return 200 with list of outbox events")
    void testGetOutboxEvents() throws Exception {
        com.payflow.order.entity.OutboxEvent event = new com.payflow.order.entity.OutboxEvent(
                "OrderCreated", "Order", "1", "{\"orderId\":1}"
        );
        event.setId(1L);

        when(orderService.getOutboxEvents(any())).thenReturn(List.of(event));

        mockMvc.perform(get("/api/orders/outbox"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(1))
                .andExpect(jsonPath("$[0].eventType").value("OrderCreated"))
                .andExpect(jsonPath("$[0].status").value("NEW"));
    }
}
