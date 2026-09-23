package com.payflow.saga.service;

import com.payflow.saga.command.InventoryCommand;
import com.payflow.saga.command.OrderCommand;
import com.payflow.saga.command.ProcessPaymentCommand;
import com.payflow.saga.entity.SagaInstance;
import com.payflow.saga.entity.SagaState;
import com.payflow.saga.entity.SagaStatus;
import com.payflow.saga.event.InventoryResultEvent;
import com.payflow.saga.event.OrderCreatedEvent;
import com.payflow.saga.event.PaymentProcessedEvent;
import com.payflow.saga.kafka.SagaCommandProducer;
import com.payflow.saga.repository.SagaInstanceRepository;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaOrchestratorServiceTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;

    @Mock
    private SagaCommandProducer sagaCommandProducer;

    @InjectMocks
    private SagaOrchestratorService sagaOrchestratorService;

    private SagaInstance sampleSaga;

    @BeforeEach
    void setUp() {
        sampleSaga = new SagaInstance("saga-123", 101L, 42L, 1001L, 1, new BigDecimal("5000.00"));
        sampleSaga.setId(1L);
    }

    @Test
    @DisplayName("Should start Saga and emit ReserveInventoryCommand on OrderCreatedEvent")
    void testHandleOrderCreated_Success() {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-1", "saga-123", 101L, 42L, 1001L, 2, new BigDecimal("5000.00"), false, false, LocalDateTime.now()
        );

        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handleOrderCreated(event);

        assertNotNull(result);
        assertEquals("saga-123", result.getSagaId());
        assertEquals(SagaState.INVENTORY_RESERVATION_PENDING, result.getCurrentState());
        assertEquals(SagaStatus.IN_PROGRESS, result.getStatus());

        ArgumentCaptor<InventoryCommand> captor = ArgumentCaptor.forClass(InventoryCommand.class);
        verify(sagaCommandProducer).sendInventoryCommand(captor.capture());
        assertEquals("RESERVE", captor.getValue().getCommandType());
        assertEquals(1001L, captor.getValue().getProductId());
        assertEquals(2, captor.getValue().getQuantity());
    }

    @Test
    @DisplayName("Should transition to PAYMENT_PENDING and emit ProcessPaymentCommand when inventory reserved")
    void testHandleInventoryResult_Reserved() {
        InventoryResultEvent event = new InventoryResultEvent(
                "evt-2", "saga-123", 101L, 1001L, 1, "RESERVED", null, LocalDateTime.now()
        );

        when(sagaInstanceRepository.findBySagaId("saga-123")).thenReturn(Optional.of(sampleSaga));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handleInventoryResult(event);

        assertNotNull(result);
        assertEquals(SagaState.PAYMENT_PENDING, result.getCurrentState());

        ArgumentCaptor<ProcessPaymentCommand> captor = ArgumentCaptor.forClass(ProcessPaymentCommand.class);
        verify(sagaCommandProducer).sendProcessPaymentCommand(captor.capture());
        assertEquals(101L, captor.getValue().getOrderId());
        assertEquals(new BigDecimal("5000.00"), captor.getValue().getAmount());
    }

    @Test
    @DisplayName("Should cancel order and fail Saga when inventory reservation fails")
    void testHandleInventoryResult_ReservationFailed() {
        InventoryResultEvent event = new InventoryResultEvent(
                "evt-3", "saga-123", 101L, 1001L, 1, "RESERVATION_FAILED", "INSUFFICIENT_STOCK", LocalDateTime.now()
        );

        when(sagaInstanceRepository.findBySagaId("saga-123")).thenReturn(Optional.of(sampleSaga));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handleInventoryResult(event);

        assertNotNull(result);
        assertEquals(SagaState.FAILED, result.getCurrentState());
        assertEquals(SagaStatus.FAILED, result.getStatus());

        ArgumentCaptor<OrderCommand> captor = ArgumentCaptor.forClass(OrderCommand.class);
        verify(sagaCommandProducer).sendOrderCommand(captor.capture());
        assertEquals("CANCEL", captor.getValue().getCommandType());
        verify(sagaCommandProducer, never()).sendProcessPaymentCommand(any());
    }

    @Test
    @DisplayName("Should confirm order and complete Saga when payment succeeds")
    void testHandlePaymentResult_Success() {
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "evt-4", "saga-123", 101L, 501L, new BigDecimal("5000.00"), "SUCCESS", LocalDateTime.now()
        );

        when(sagaInstanceRepository.findBySagaId("saga-123")).thenReturn(Optional.of(sampleSaga));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handlePaymentResult(event);

        assertNotNull(result);
        assertEquals(SagaState.COMPLETED, result.getCurrentState());
        assertEquals(SagaStatus.COMPLETED, result.getStatus());

        ArgumentCaptor<OrderCommand> captor = ArgumentCaptor.forClass(OrderCommand.class);
        verify(sagaCommandProducer).sendOrderCommand(captor.capture());
        assertEquals("CONFIRM", captor.getValue().getCommandType());
    }

    @Test
    @DisplayName("Should trigger inventory release compensation when payment fails")
    void testHandlePaymentResult_PaymentFailed() {
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "evt-5", "saga-123", 101L, 502L, new BigDecimal("5000.00"), "FAILED", LocalDateTime.now()
        );

        when(sagaInstanceRepository.findBySagaId("saga-123")).thenReturn(Optional.of(sampleSaga));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handlePaymentResult(event);

        assertNotNull(result);
        assertEquals(SagaState.COMPENSATING_INVENTORY, result.getCurrentState());

        ArgumentCaptor<InventoryCommand> captor = ArgumentCaptor.forClass(InventoryCommand.class);
        verify(sagaCommandProducer).sendInventoryCommand(captor.capture());
        assertEquals("RELEASE", captor.getValue().getCommandType());
        assertEquals(1001L, captor.getValue().getProductId());
    }

    @Test
    @DisplayName("Should cancel order and mark Saga COMPENSATED when inventory release succeeds")
    void testHandleInventoryResult_Released_CompensationComplete() {
        sampleSaga.setCurrentState(SagaState.COMPENSATING_INVENTORY);
        InventoryResultEvent event = new InventoryResultEvent(
                "evt-6", "saga-123", 101L, 1001L, 1, "RELEASED", null, LocalDateTime.now()
        );

        when(sagaInstanceRepository.findBySagaId("saga-123")).thenReturn(Optional.of(sampleSaga));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(i -> i.getArgument(0));

        SagaInstance result = sagaOrchestratorService.handleInventoryResult(event);

        assertNotNull(result);
        assertEquals(SagaState.FAILED, result.getCurrentState());
        assertEquals(SagaStatus.COMPENSATED, result.getStatus());

        ArgumentCaptor<OrderCommand> captor = ArgumentCaptor.forClass(OrderCommand.class);
        verify(sagaCommandProducer).sendOrderCommand(captor.capture());
        assertEquals("CANCEL", captor.getValue().getCommandType());
    }
}
