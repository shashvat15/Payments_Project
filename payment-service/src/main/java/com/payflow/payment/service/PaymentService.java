package com.payflow.payment.service;

import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.Payment;
import com.payflow.payment.entity.PaymentStatus;
import com.payflow.payment.event.OrderCreatedEvent;
import com.payflow.payment.event.PaymentProcessedEvent;
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

    /**
     * Phase 2B Saga: Processes payment triggered by Saga Orchestrator via ProcessPaymentCommand.
     * Persists payment in payment_db and returns PaymentProcessedEvent containing sagaId to publish to payment-result.
     */
    @Transactional
    public PaymentProcessedEvent processPaymentFromCommand(com.payflow.payment.command.ProcessPaymentCommand command) {
        log.info("[Saga: {}] Processing payment from command for orderId: {}, amount: {}, simulateFailure: {}",
                command.getSagaId(), command.getOrderId(), command.getAmount(), command.getSimulatePaymentFailure());

        PaymentStatus status = Boolean.TRUE.equals(command.getSimulatePaymentFailure())
                ? PaymentStatus.FAILED
                : PaymentStatus.SUCCESS;

        Payment payment = new Payment(
                command.getOrderId(),
                command.getAmount(),
                status
        );

        Payment savedPayment = paymentRepository.save(payment);
        log.info("[Saga: {}] Payment saved in payment_db with id: {}, status: {}",
                command.getSagaId(), savedPayment.getId(), savedPayment.getStatus());

        return new PaymentProcessedEvent(
                command.getSagaId(),
                savedPayment.getOrderId(),
                savedPayment.getId(),
                savedPayment.getAmount(),
                savedPayment.getStatus().name()
        );
    }

    /**
     * Processes payment triggered asynchronously via Kafka OrderCreatedEvent.
     * Persists payment in payment_db and returns PaymentProcessedEvent to be published back to Kafka.
     */
    @Transactional
    public PaymentProcessedEvent processPaymentFromEvent(OrderCreatedEvent event) {
        log.info("Processing payment from Kafka event for orderId: {}, amount: {}, simulateFailure: {}",
                event.getOrderId(), event.getAmount(), event.getSimulatePaymentFailure());

        PaymentStatus status = Boolean.TRUE.equals(event.getSimulatePaymentFailure())
                ? PaymentStatus.FAILED
                : PaymentStatus.SUCCESS;

        Payment payment = new Payment(
                event.getOrderId(),
                event.getAmount(),
                status
        );

        Payment savedPayment = paymentRepository.save(payment);
        log.info("Payment saved in payment_db with id: {}, status: {}", savedPayment.getId(), savedPayment.getStatus());

        return new PaymentProcessedEvent(
                savedPayment.getOrderId(),
                savedPayment.getId(),
                savedPayment.getAmount(),
                savedPayment.getStatus().name()
        );
    }

    @Transactional
    public PaymentResponse processPayment(ProcessPaymentRequest request) {
        log.info("Processing payment for orderId: {}, amount: {}, simulateFailure: {}",
                request.getOrderId(), request.getAmount(), request.getSimulateFailure());

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
