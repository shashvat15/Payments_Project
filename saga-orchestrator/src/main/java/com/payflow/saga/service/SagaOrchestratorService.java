package com.payflow.saga.service;

import com.payflow.saga.command.InventoryCommand;
import com.payflow.saga.command.OrderCommand;
import com.payflow.saga.command.ProcessPaymentCommand;
import com.payflow.saga.dto.SagaResponse;
import com.payflow.saga.entity.SagaInstance;
import com.payflow.saga.entity.SagaState;
import com.payflow.saga.entity.SagaStatus;
import com.payflow.saga.event.InventoryResultEvent;
import com.payflow.saga.event.OrderCreatedEvent;
import com.payflow.saga.event.PaymentProcessedEvent;
import com.payflow.saga.kafka.SagaCommandProducer;
import com.payflow.saga.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class SagaOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestratorService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaCommandProducer sagaCommandProducer;

    public SagaOrchestratorService(SagaInstanceRepository sagaInstanceRepository, SagaCommandProducer sagaCommandProducer) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaCommandProducer = sagaCommandProducer;
    }

    /**
     * Step 1: Start Saga on OrderCreated event.
     * Transitions: (none) -> STARTED -> INVENTORY_RESERVATION_PENDING
     * Dispatches: ReserveInventory command to Inventory Service
     */
    @Transactional
    public SagaInstance handleOrderCreated(OrderCreatedEvent event) {
        String sagaId = (event.getSagaId() != null && !event.getSagaId().isBlank())
                ? event.getSagaId()
                : UUID.randomUUID().toString();

        log.info("[Saga: {}] Received OrderCreatedEvent: orderId={}, customerId={}, amount={}, productId={}, quantity={}",
                sagaId, event.getOrderId(), event.getCustomerId(), event.getAmount(), event.getProductId(), event.getQuantity());

        Long productId = event.getProductId() != null ? event.getProductId() : 1001L;
        Integer quantity = event.getQuantity() != null ? event.getQuantity() : 1;

        SagaInstance saga = new SagaInstance(
                sagaId,
                event.getOrderId(),
                event.getCustomerId(),
                productId,
                quantity,
                event.getAmount()
        );
        saga.setSimulatePaymentFailure(event.getSimulatePaymentFailure());
        saga.setCurrentState(SagaState.STARTED);
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga = sagaInstanceRepository.save(saga);

        log.info("[Saga: {}] Saga initialized in STARTED state for order #{}", sagaId, event.getOrderId());

        // Dispatch ReserveInventory command
        InventoryCommand reserveCmd = InventoryCommand.reserve(
                sagaId,
                event.getOrderId(),
                productId,
                quantity,
                event.getSimulateInventoryFailure()
        );

        saga.setCurrentState(SagaState.INVENTORY_RESERVATION_PENDING);
        saga = sagaInstanceRepository.save(saga);

        log.info("[Saga: {}] State transitioned to INVENTORY_RESERVATION_PENDING. Emitting ReserveInventoryCommand", sagaId);
        sagaCommandProducer.sendInventoryCommand(reserveCmd);

        return saga;
    }

    /**
     * Step 2: Handle Inventory result (RESERVED, RESERVATION_FAILED, or RELEASED)
     */
    @Transactional
    public SagaInstance handleInventoryResult(InventoryResultEvent event) {
        String sagaId = event.getSagaId();
        log.info("[Saga: {}] Received InventoryResultEvent: status={}, orderId={}, productId={}, quantity={}",
                sagaId, event.getStatus(), event.getOrderId(), event.getProductId(), event.getQuantity());

        SagaInstance saga = findSaga(sagaId, event.getOrderId());
        if (saga == null) {
            log.error("[Saga: {}] No SagaInstance found for order #{}", sagaId, event.getOrderId());
            return null;
        }

        if ("RESERVED".equalsIgnoreCase(event.getStatus())) {
            // Inventory reservation succeeded -> Transition to INVENTORY_RESERVED, then PAYMENT_PENDING
            saga.setCurrentState(SagaState.INVENTORY_RESERVED);
            log.info("[Saga: {}] State transitioned to INVENTORY_RESERVED", sagaId);

            ProcessPaymentCommand paymentCmd = new ProcessPaymentCommand(
                    saga.getSagaId(),
                    saga.getOrderId(),
                    saga.getCustomerId(),
                    saga.getAmount(),
                    saga.getSimulatePaymentFailure()
            );

            saga.setCurrentState(SagaState.PAYMENT_PENDING);
            saga = sagaInstanceRepository.save(saga);

            log.info("[Saga: {}] State transitioned to PAYMENT_PENDING. Emitting ProcessPaymentCommand: amount={}",
                    sagaId, saga.getAmount());
            sagaCommandProducer.sendProcessPaymentCommand(paymentCmd);

        } else if ("RESERVATION_FAILED".equalsIgnoreCase(event.getStatus())) {
            // Inventory reservation failed -> Transition to ORDER_CANCELLATION_PENDING, cancel order, end Saga as FAILED
            log.warn("[Saga: {}] Inventory reservation failed: reason={}. Triggering order cancellation",
                    sagaId, event.getReason());

            saga.setFailureReason("Inventory reservation failed: " + event.getReason());
            saga.setCurrentState(SagaState.ORDER_CANCELLATION_PENDING);

            OrderCommand cancelCmd = OrderCommand.cancel(sagaId, saga.getOrderId(), saga.getFailureReason());
            sagaCommandProducer.sendOrderCommand(cancelCmd);

            saga.setCurrentState(SagaState.FAILED);
            saga.setStatus(SagaStatus.FAILED);
            saga = sagaInstanceRepository.save(saga);

            log.info("[Saga: {}] Saga terminated with FAILED status (Inventory reservation failed)", sagaId);

        } else if ("RELEASED".equalsIgnoreCase(event.getStatus())) {
            // Compensation step: Inventory released after payment failure -> Cancel order, end Saga as COMPENSATED
            log.info("[Saga: {}] Inventory release acknowledged. Proceeding to cancel order #{}", sagaId, saga.getOrderId());

            saga.setCurrentState(SagaState.ORDER_CANCELLATION_PENDING);

            OrderCommand cancelCmd = OrderCommand.cancel(sagaId, saga.getOrderId(), "Payment processing failed; inventory compensated");
            sagaCommandProducer.sendOrderCommand(cancelCmd);

            saga.setCurrentState(SagaState.FAILED);
            saga.setStatus(SagaStatus.COMPENSATED);
            saga = sagaInstanceRepository.save(saga);

            log.info("[Saga: {}] Compensation complete! Saga finished with COMPENSATED status", sagaId);
        }

        return saga;
    }

    /**
     * Step 3: Handle Payment result (SUCCESS or FAILED)
     */
    @Transactional
    public SagaInstance handlePaymentResult(PaymentProcessedEvent event) {
        String sagaId = event.getSagaId();
        log.info("[Saga: {}] Received PaymentProcessedEvent: status={}, paymentId={}, orderId={}, amount={}",
                sagaId, event.getStatus(), event.getPaymentId(), event.getOrderId(), event.getAmount());

        SagaInstance saga = findSaga(sagaId, event.getOrderId());
        if (saga == null) {
            log.error("[Saga: {}] No SagaInstance found for order #{}", sagaId, event.getOrderId());
            return null;
        }

        if ("SUCCESS".equalsIgnoreCase(event.getStatus())) {
            // Payment succeeded -> Confirm order and complete Saga
            saga.setCurrentState(SagaState.PAYMENT_SUCCEEDED);
            log.info("[Saga: {}] State transitioned to PAYMENT_SUCCEEDED", sagaId);

            OrderCommand confirmCmd = OrderCommand.confirm(saga.getSagaId(), saga.getOrderId());
            sagaCommandProducer.sendOrderCommand(confirmCmd);

            saga.setCurrentState(SagaState.COMPLETED);
            saga.setStatus(SagaStatus.COMPLETED);
            saga = sagaInstanceRepository.save(saga);

            log.info("[Saga: {}] Order confirmed. Saga successfully COMPLETED!", sagaId);

        } else {
            // Payment failed -> Trigger COMPENSATING TRANSACTION: Release reserved inventory
            log.warn("[Saga: {}] Payment failed for order #{}. INITIATING COMPENSATION: Releasing reserved inventory",
                    sagaId, saga.getOrderId());

            saga.setFailureReason("Payment failed for order #" + saga.getOrderId());
            saga.setCurrentState(SagaState.COMPENSATING_INVENTORY);
            saga = sagaInstanceRepository.save(saga);

            InventoryCommand releaseCmd = InventoryCommand.release(
                    saga.getSagaId(),
                    saga.getOrderId(),
                    saga.getProductId(),
                    saga.getQuantity()
            );

            log.info("[Saga: {}] State transitioned to COMPENSATING_INVENTORY. Emitting ReleaseInventoryCommand", sagaId);
            sagaCommandProducer.sendInventoryCommand(releaseCmd);
        }

        return saga;
    }

    private SagaInstance findSaga(String sagaId, Long orderId) {
        if (sagaId != null && !sagaId.isBlank()) {
            Optional<SagaInstance> opt = sagaInstanceRepository.findBySagaId(sagaId);
            if (opt.isPresent()) {
                return opt.get();
            }
        }
        if (orderId != null) {
            List<SagaInstance> list = sagaInstanceRepository.findByOrderId(orderId);
            if (!list.isEmpty()) {
                return list.get(list.size() - 1);
            }
        }
        return null;
    }

    @Transactional(readOnly = true)
    public Optional<SagaResponse> getSagaById(String sagaId) {
        return sagaInstanceRepository.findBySagaId(sagaId).map(this::mapToResponse);
    }

    @Transactional(readOnly = true)
    public List<SagaResponse> getSagasByOrderId(Long orderId) {
        return sagaInstanceRepository.findByOrderId(orderId).stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<SagaResponse> getAllSagas() {
        return sagaInstanceRepository.findAll().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private SagaResponse mapToResponse(SagaInstance s) {
        return new SagaResponse(
                s.getId(),
                s.getSagaId(),
                s.getOrderId(),
                s.getCustomerId(),
                s.getProductId(),
                s.getQuantity(),
                s.getAmount(),
                s.getCurrentState(),
                s.getStatus(),
                s.getFailureReason(),
                s.getCreatedAt(),
                s.getUpdatedAt()
        );
    }
}
