package com.payflow.payment.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.PaymentStatus;
import com.payflow.payment.exception.ResourceNotFoundException;
import com.payflow.payment.service.PaymentService;
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

@WebMvcTest(PaymentController.class)
class PaymentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private PaymentService paymentService;

    @Test
    @DisplayName("POST /api/payments should return 201 with PaymentResponse")
    void testProcessPayment_Success() throws Exception {
        ProcessPaymentRequest request = new ProcessPaymentRequest(101L, new BigDecimal("5000.00"), false);
        PaymentResponse response = new PaymentResponse(
                1L, 101L, new BigDecimal("5000.00"), PaymentStatus.SUCCESS,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(paymentService.processPayment(any(ProcessPaymentRequest.class))).thenReturn(response);

        mockMvc.perform(post("/api/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.orderId").value(101))
                .andExpect(jsonPath("$.amount").value(5000.00))
                .andExpect(jsonPath("$.status").value("SUCCESS"));
    }

    @Test
    @DisplayName("POST /api/payments with invalid request should return 400 Bad Request")
    void testProcessPayment_ValidationFailure() throws Exception {
        ProcessPaymentRequest invalidRequest = new ProcessPaymentRequest(null, new BigDecimal("-10.00"), false);

        mockMvc.perform(post("/api/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidRequest)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.validationErrors.orderId").exists())
                .andExpect(jsonPath("$.validationErrors.amount").exists());
    }

    @Test
    @DisplayName("GET /api/payments/{id} should return 200 when found")
    void testGetPaymentById_Found() throws Exception {
        PaymentResponse response = new PaymentResponse(
                1L, 101L, new BigDecimal("5000.00"), PaymentStatus.SUCCESS,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(paymentService.getPaymentById(1L)).thenReturn(response);

        mockMvc.perform(get("/api/payments/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.status").value("SUCCESS"));
    }

    @Test
    @DisplayName("GET /api/payments/{id} should return 404 when not found")
    void testGetPaymentById_NotFound() throws Exception {
        when(paymentService.getPaymentById(999L))
                .thenThrow(new ResourceNotFoundException("Payment not found with id: 999"));

        mockMvc.perform(get("/api/payments/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("Payment not found with id: 999"));
    }

    @Test
    @DisplayName("GET /api/payments/order/{orderId} should return 200 with list of payments")
    void testGetPaymentsByOrderId() throws Exception {
        PaymentResponse response = new PaymentResponse(
                1L, 101L, new BigDecimal("5000.00"), PaymentStatus.SUCCESS,
                LocalDateTime.now(), LocalDateTime.now()
        );

        when(paymentService.getPaymentsByOrderId(101L)).thenReturn(List.of(response));

        mockMvc.perform(get("/api/payments/order/101"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderId").value(101))
                .andExpect(jsonPath("$[0].status").value("SUCCESS"));
    }
}
