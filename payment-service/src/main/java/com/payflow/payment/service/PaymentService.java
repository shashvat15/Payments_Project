package com.payflow.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payment.command.ProcessPaymentCommand;
import com.payflow.payment.dto.PaymentResponse;
import com.payflow.payment.dto.ProcessPaymentRequest;
import com.payflow.payment.entity.OutboxEvent;
import com.payflow.payment.entity.Payment;
import com.payflow.payment.entity.PaymentStatus;
import com.payflow.payment.event.OrderCreatedEvent;
import com.payflow.payment.event.PaymentProcessedEvent;
import com.payflow.payment.exception.ResourceNotFoundException;
import com.payflow.payment.repository.OutboxEventRepository;
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
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public PaymentService(
            PaymentRepository paymentRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Phase 2C Transactional Outbox Pattern:
     * 1. Persists Payment (SUCCESS or FAILED) in payment_db.
     * 2. Constructs PaymentProcessedEvent contract.
     * 3. Persists OutboxEvent in payment_db with status NEW.
     * 4. Payment and OutboxEvent are committed together in the SAME local database transaction.
     * 5. Kafka publication is delegated to OutboxPublisher.
     */
    @Transactional
    public PaymentProcessedEvent processPaymentFromCommand(ProcessPaymentCommand command) {
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

        PaymentProcessedEvent event = new PaymentProcessedEvent(
                command.getSagaId(),
                savedPayment.getOrderId(),
                savedPayment.getId(),
                savedPayment.getAmount(),
                savedPayment.getStatus().name()
        );

        // Save OutboxEvent in the same local database transaction
        try {
            String payload = objectMapper.writeValueAsString(event);
            OutboxEvent outboxEvent = new OutboxEvent(
                    "PaymentProcessed",
                    "Payment",
                    String.valueOf(savedPayment.getId()),
                    "payment-result",
                    String.valueOf(savedPayment.getOrderId()),
                    payload
            );
            outboxEventRepository.save(outboxEvent);
            log.info("[Saga: {}] OutboxEvent saved in payment_db for payment #{} [orderId: {}] with status NEW",
                    command.getSagaId(), savedPayment.getId(), savedPayment.getOrderId());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize PaymentProcessedEvent for orderId: {}", savedPayment.getOrderId(), e);
            throw new RuntimeException("Failed to serialize outbox event payload: " + e.getMessage(), e);
        }

        return event;
    }

    /**
     * Processes payment triggered asynchronously via Kafka OrderCreatedEvent (Deprecated Phase 2A Choreography).
     * Persists payment in payment_db and persists OutboxEvent to be published back to Kafka.
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

        PaymentProcessedEvent processedEvent = new PaymentProcessedEvent(
                savedPayment.getOrderId(),
                savedPayment.getId(),
                savedPayment.getAmount(),
                savedPayment.getStatus().name()
        );

        try {
            String payload = objectMapper.writeValueAsString(processedEvent);
            OutboxEvent outboxEvent = new OutboxEvent(
                    "PaymentProcessed",
                    "Payment",
                    String.valueOf(savedPayment.getId()),
                    "payment-result",
                    String.valueOf(savedPayment.getOrderId()),
                    payload
            );
            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize PaymentProcessedEvent for orderId: {}", savedPayment.getOrderId(), e);
            throw new RuntimeException("Failed to serialize outbox event payload: " + e.getMessage(), e);
        }

        return processedEvent;
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

    @Transactional(readOnly = true)
    public List<OutboxEvent> getOutboxEvents() {
        return outboxEventRepository.findAll();
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
