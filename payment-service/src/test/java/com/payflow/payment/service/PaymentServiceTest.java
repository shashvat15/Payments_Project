package com.payflow.payment.service;

import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.Payment;
import com.payflow.payment.entity.PaymentStatus;
import com.payflow.payment.exception.ResourceNotFoundException;
import com.payflow.payment.repository.PaymentRepository;
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

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @InjectMocks
    private PaymentService paymentService;

    private Payment samplePayment;

    @BeforeEach
    void setUp() {
        samplePayment = new Payment(101L, new BigDecimal("5000.00"), PaymentStatus.SUCCESS);
        samplePayment.setId(1L);
        samplePayment.setCreatedAt(LocalDateTime.now());
        samplePayment.setUpdatedAt(LocalDateTime.now());
    }

    @Test
    @DisplayName("Should process payment successfully by default")
    void testProcessPayment_Success() {
        ProcessPaymentRequest request = new ProcessPaymentRequest(101L, new BigDecimal("5000.00"), false);
        when(paymentRepository.save(any(Payment.class))).thenReturn(samplePayment);

        PaymentResponse response = paymentService.processPayment(request);

        assertNotNull(response);
        assertEquals(1L, response.getId());
        assertEquals(101L, response.getOrderId());
        assertEquals(new BigDecimal("5000.00"), response.getAmount());
        assertEquals(PaymentStatus.SUCCESS, response.getStatus());

        ArgumentCaptor<Payment> captor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(captor.capture());
        assertEquals(PaymentStatus.SUCCESS, captor.getValue().getStatus());
        assertEquals(101L, captor.getValue().getOrderId());
    }

    @Test
    @DisplayName("Should record FAILED payment when simulateFailure is true")
    void testProcessPayment_SimulatedFailure() {
        ProcessPaymentRequest request = new ProcessPaymentRequest(101L, new BigDecimal("5000.00"), true);
        Payment failedPayment = new Payment(101L, new BigDecimal("5000.00"), PaymentStatus.FAILED);
        failedPayment.setId(2L);
        failedPayment.setCreatedAt(LocalDateTime.now());

        when(paymentRepository.save(any(Payment.class))).thenReturn(failedPayment);

        PaymentResponse response = paymentService.processPayment(request);

        assertNotNull(response);
        assertEquals(2L, response.getId());
        assertEquals(PaymentStatus.FAILED, response.getStatus());

        ArgumentCaptor<Payment> captor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(captor.capture());
        assertEquals(PaymentStatus.FAILED, captor.getValue().getStatus());
    }

    @Test
    @DisplayName("Should fetch payment by ID")
    void testGetPaymentById_Found() {
        when(paymentRepository.findById(1L)).thenReturn(Optional.of(samplePayment));

        PaymentResponse response = paymentService.getPaymentById(1L);

        assertNotNull(response);
        assertEquals(1L, response.getId());
        assertEquals(PaymentStatus.SUCCESS, response.getStatus());
    }

    @Test
    @DisplayName("Should throw ResourceNotFoundException when payment ID not found")
    void testGetPaymentById_NotFound() {
        when(paymentRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> paymentService.getPaymentById(999L));
    }

    @Test
    @DisplayName("Should fetch payments by order ID")
    void testGetPaymentsByOrderId() {
        when(paymentRepository.findByOrderId(101L)).thenReturn(List.of(samplePayment));

        List<PaymentResponse> responses = paymentService.getPaymentsByOrderId(101L);

        assertEquals(1, responses.size());
        assertEquals(101L, responses.get(0).getOrderId());
    }
}
