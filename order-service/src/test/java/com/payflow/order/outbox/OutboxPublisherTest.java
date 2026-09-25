package com.payflow.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.payflow.order.entity.OutboxEvent;
import com.payflow.order.entity.OutboxStatus;
import com.payflow.order.event.OrderCreatedEvent;
import com.payflow.order.kafka.OrderEventProducer;
import com.payflow.order.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private OrderEventProducer orderEventProducer;

    private ObjectMapper objectMapper;
    private OutboxPublisher outboxPublisher;

    private OutboxEvent sampleOutboxEvent;
    private OrderCreatedEvent samplePayloadEvent;

    @BeforeEach
    void setUp() throws Exception {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        outboxPublisher = new OutboxPublisher(outboxEventRepository, orderEventProducer, objectMapper, 5000);

        samplePayloadEvent = new OrderCreatedEvent(
                "evt-101",
                "saga-101",
                101L,
                42L,
                1001L,
                2,
                new BigDecimal("5000.00"),
                false,
                false,
                LocalDateTime.now()
        );

        String jsonPayload = objectMapper.writeValueAsString(samplePayloadEvent);

        sampleOutboxEvent = new OutboxEvent(
                "OrderCreated",
                "Order",
                "101",
                jsonPayload
        );
        sampleOutboxEvent.setId(1L);
        sampleOutboxEvent.setCreatedAt(LocalDateTime.now());
    }

    @Test
    @DisplayName("Should publish NEW outbox event to Kafka and update status to PUBLISHED")
    void testPublishPendingEvents_Success() {
        when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW))
                .thenReturn(List.of(sampleOutboxEvent));

        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("order-created", 0), 0, 0, System.currentTimeMillis(), 0, 0
        );
        SendResult<String, Object> sendResult = new SendResult<>(null, recordMetadata);
        when(orderEventProducer.sendOrderCreatedEvent(any(OrderCreatedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        int publishedCount = outboxPublisher.publishPendingEvents();

        assertEquals(1, publishedCount);
        assertEquals(OutboxStatus.PUBLISHED, sampleOutboxEvent.getStatus());
        assertNotNull(sampleOutboxEvent.getPublishedAt());

        ArgumentCaptor<OrderCreatedEvent> eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(orderEventProducer).sendOrderCreatedEvent(eventCaptor.capture());
        assertEquals(101L, eventCaptor.getValue().getOrderId());
        assertEquals("saga-101", eventCaptor.getValue().getSagaId());

        verify(outboxEventRepository).save(sampleOutboxEvent);
    }

    @Test
    @DisplayName("Should leave status as NEW for retry when Kafka publication fails")
    void testPublishPendingEvents_KafkaFailure_LeavesStatusAsNew() {
        when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW))
                .thenReturn(List.of(sampleOutboxEvent));

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker unreachable (simulated)"));
        when(orderEventProducer.sendOrderCreatedEvent(any(OrderCreatedEvent.class)))
                .thenReturn(failedFuture);

        int publishedCount = outboxPublisher.publishPendingEvents();

        assertEquals(0, publishedCount);
        assertEquals(OutboxStatus.NEW, sampleOutboxEvent.getStatus());
        assertNull(sampleOutboxEvent.getPublishedAt());

        // Event should NOT be marked PUBLISHED
        verify(outboxEventRepository, never()).save(argThat(e -> e.getStatus() == OutboxStatus.PUBLISHED));
    }

    @Test
    @DisplayName("Should retry previously failed event on next poll cycle and succeed")
    void testPublishPendingEvents_RetrySucceeds() {
        // First cycle: Kafka fails
        when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW))
                .thenReturn(List.of(sampleOutboxEvent));

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Connection timed out"));
        when(orderEventProducer.sendOrderCreatedEvent(any(OrderCreatedEvent.class)))
                .thenReturn(failedFuture);

        int cycle1Count = outboxPublisher.publishPendingEvents();
        assertEquals(0, cycle1Count);
        assertEquals(OutboxStatus.NEW, sampleOutboxEvent.getStatus());

        // Second cycle: Kafka is back online, retry succeeds
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("order-created", 0), 0, 1, System.currentTimeMillis(), 0, 0
        );
        SendResult<String, Object> sendResult = new SendResult<>(null, recordMetadata);
        when(orderEventProducer.sendOrderCreatedEvent(any(OrderCreatedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        int cycle2Count = outboxPublisher.publishPendingEvents();
        assertEquals(1, cycle2Count);
        assertEquals(OutboxStatus.PUBLISHED, sampleOutboxEvent.getStatus());
        assertNotNull(sampleOutboxEvent.getPublishedAt());
        verify(outboxEventRepository).save(sampleOutboxEvent);
    }

    @Test
    @DisplayName("Should return 0 when no NEW events are found in outbox table")
    void testPublishPendingEvents_NoNewEvents() {
        when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW))
                .thenReturn(Collections.emptyList());

        int publishedCount = outboxPublisher.publishPendingEvents();

        assertEquals(0, publishedCount);
        verify(orderEventProducer, never()).sendOrderCreatedEvent(any());
        verify(outboxEventRepository, never()).save(any());
    }
}
