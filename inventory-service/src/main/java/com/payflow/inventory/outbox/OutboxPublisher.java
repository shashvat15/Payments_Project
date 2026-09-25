package com.payflow.inventory.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.inventory.entity.OutboxEvent;
import com.payflow.inventory.entity.OutboxStatus;
import com.payflow.inventory.event.InventoryResultEvent;
import com.payflow.inventory.kafka.InventoryEventProducer;
import com.payflow.inventory.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final InventoryEventProducer inventoryEventProducer;
    private final ObjectMapper objectMapper;
    private final long publishTimeoutMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            InventoryEventProducer inventoryEventProducer,
            ObjectMapper objectMapper,
            @Value("${app.outbox.publisher.timeout-ms:5000}") long publishTimeoutMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.inventoryEventProducer = inventoryEventProducer;
        this.objectMapper = objectMapper;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${app.outbox.publisher.fixed-delay:2000}")
    public void scheduledPublish() {
        publishPendingEvents();
    }

    public int publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.NEW);
        if (pendingEvents.isEmpty()) {
            return 0;
        }

        log.debug("[Inventory-Outbox] Discovered {} unpublished event(s)", pendingEvents.size());
        int publishedCount = 0;

        for (OutboxEvent event : pendingEvents) {
            boolean success = publishSingleEvent(event);
            if (success) {
                publishedCount++;
            }
        }

        return publishedCount;
    }

    public boolean publishSingleEvent(OutboxEvent outboxEvent) {
        log.info("[Inventory-Outbox] Discovered event to publish: id={}, aggregateId={}, eventType={}, status={}",
                outboxEvent.getId(), outboxEvent.getAggregateId(), outboxEvent.getEventType(), outboxEvent.getStatus());

        try {
            if ("InventoryResult".equalsIgnoreCase(outboxEvent.getEventType()) ||
                "InventoryReserved".equalsIgnoreCase(outboxEvent.getEventType()) ||
                "InventoryReservationFailed".equalsIgnoreCase(outboxEvent.getEventType()) ||
                "InventoryReleased".equalsIgnoreCase(outboxEvent.getEventType())) {

                InventoryResultEvent eventPayload = objectMapper.readValue(outboxEvent.getPayload(), InventoryResultEvent.class);
                log.info("[Inventory-Outbox] Publishing event: id={}, orderId={}, sagaId={}, status={}",
                        outboxEvent.getId(), eventPayload.getOrderId(), eventPayload.getSagaId(), eventPayload.getStatus());

                CompletableFuture<SendResult<String, Object>> future = inventoryEventProducer.sendInventoryResultEvent(eventPayload);
                SendResult<String, Object> sendResult = future.get(publishTimeoutMs, TimeUnit.MILLISECONDS);

                outboxEvent.setStatus(OutboxStatus.PUBLISHED);
                outboxEvent.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(outboxEvent);

                log.info("[Inventory-Outbox] Publication succeeded: id={}, partition={}, offset={}",
                        outboxEvent.getId(),
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().partition() : -1,
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().offset() : -1);
                return true;
            } else {
                log.warn("[Inventory-Outbox] Unknown event type '{}' for outbox event id={}",
                        outboxEvent.getEventType(), outboxEvent.getId());
                return false;
            }
        } catch (Exception ex) {
            log.error("[Inventory-Outbox] Publication failed for event id={} [aggregateId={}]: {}. Retrying on next run.",
                    outboxEvent.getId(), outboxEvent.getAggregateId(), ex.getMessage());
            // Record remains NEW, will be retried on next scheduled cycle
            return false;
        }
    }
}
