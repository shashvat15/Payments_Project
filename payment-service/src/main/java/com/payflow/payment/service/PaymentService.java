package com.payflow.payment.service;

import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.Payment;
import com.payflow.payment.entity.PaymentStatus;
import com.payflow.payment.exception.ResourceNotFoundException;
import com.payflow.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Transactional
    public PaymentResponse processPayment(ProcessPaymentRequest request) {
        log.info("Processing payment for orderId: {}, amount: {}, simulateFailure: {}",
                request.getOrderId(), request.getAmount(), request.getSimulateFailure());

        // Determine simulated payment outcome
        PaymentStatus initialStatus = Boolean.TRUE.equals(request.getSimulateFailure())
                ? PaymentStatus.FAILED
                : PaymentStatus.SUCCESS;

        Payment payment = new Payment(
                request.getOrderId(),
                request.getAmount(),
                initialStatus
        );

        Payment savedPayment = paymentRepository.save(payment);
        log.info("Payment saved with id: {}, status: {}", savedPayment.getId(), savedPayment.getStatus());

        return mapToResponse(savedPayment);
    }

    @Transactional(readOnly = true)
    public PaymentResponse getPaymentById(Long id) {
        Payment payment = paymentRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Payment not found with id: " + id));
        return mapToResponse(payment);
    }

    @Transactional(readOnly = true)
    public List<PaymentResponse> getPaymentsByOrderId(Long orderId) {
        List<Payment> payments = paymentRepository.findByOrderId(orderId);
        return payments.stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private PaymentResponse mapToResponse(Payment payment) {
        return new PaymentResponse(
                payment.getId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getStatus(),
                payment.getCreatedAt(),
                payment.getUpdatedAt()
        );
    }
}
