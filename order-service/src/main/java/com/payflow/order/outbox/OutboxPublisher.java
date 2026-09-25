package com.payflow.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.order.entity.OutboxEvent;
import com.payflow.order.entity.OutboxStatus;
import com.payflow.order.event.OrderCreatedEvent;
import com.payflow.order.kafka.OrderEventProducer;
import com.payflow.order.repository.OutboxEventRepository;
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
    private final OrderEventProducer orderEventProducer;
    private final ObjectMapper objectMapper;
    private final long publishTimeoutMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            OrderEventProducer orderEventProducer,
            ObjectMapper objectMapper,
            @Value("${app.outbox.publisher.timeout-ms:5000}") long publishTimeoutMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.orderEventProducer = orderEventProducer;
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

        log.debug("[Outbox] Discovered {} unpublished event(s)", pendingEvents.size());
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
        log.info("[Outbox] Discovered event to publish: id={}, aggregateId={}, eventType={}, status={}",
                outboxEvent.getId(), outboxEvent.getAggregateId(), outboxEvent.getEventType(), outboxEvent.getStatus());

        try {
            if ("OrderCreated".equalsIgnoreCase(outboxEvent.getEventType())) {
                OrderCreatedEvent eventPayload = objectMapper.readValue(outboxEvent.getPayload(), OrderCreatedEvent.class);
                log.info("[Outbox] Publishing event: id={}, orderId={}, sagaId={}",
                        outboxEvent.getId(), eventPayload.getOrderId(), eventPayload.getSagaId());

                CompletableFuture<SendResult<String, Object>> future = orderEventProducer.sendOrderCreatedEvent(eventPayload);
                SendResult<String, Object> sendResult = future.get(publishTimeoutMs, TimeUnit.MILLISECONDS);

                outboxEvent.setStatus(OutboxStatus.PUBLISHED);
                outboxEvent.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(outboxEvent);

                log.info("[Outbox] Publication succeeded: id={}, partition={}, offset={}",
                        outboxEvent.getId(),
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().partition() : -1,
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().offset() : -1);
                return true;
            } else {
                log.warn("[Outbox] Unknown event type '{}' for outbox event id={}",
                        outboxEvent.getEventType(), outboxEvent.getId());
                return false;
            }
        } catch (Exception ex) {
            log.error("[Outbox] Publication failed for event id={} [aggregateId={}]: {}. Retrying event on next run.",
                    outboxEvent.getId(), outboxEvent.getAggregateId(), ex.getMessage());
            // Record remains NEW, will be retried on next scheduled cycle
            return false;
        }
    }
}
