package com.payflow.payment.controller;

import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.OutboxEvent;
import com.payflow.payment.service.PaymentService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> processPayment(
            @Valid @RequestBody ProcessPaymentRequest request,
            @RequestHeader(value = "X-Simulate-Failure", required = false) Boolean headerSimulateFailure) {

        log.info("Received payment request for orderId: {}", request.getOrderId());

        // Support failure simulation via request body or custom header
        if (Boolean.TRUE.equals(headerSimulateFailure)) {
            request.setSimulateFailure(true);
        }

        PaymentResponse response = paymentService.processPayment(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPaymentById(@PathVariable Long id) {
        log.info("Fetching payment with id: {}", id);
        PaymentResponse response = paymentService.getPaymentById(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<PaymentResponse>> getPaymentsByOrderId(@PathVariable Long orderId) {
        log.info("Fetching payments for orderId: {}", orderId);
        List<PaymentResponse> responses = paymentService.getPaymentsByOrderId(orderId);
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<OutboxEvent>> getOutboxEvents() {
        log.info("Fetching all outbox events in payment_db");
        return ResponseEntity.ok(paymentService.getOutboxEvents());
    }
}
