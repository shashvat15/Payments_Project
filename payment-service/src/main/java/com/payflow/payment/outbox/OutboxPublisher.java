package com.payflow.payment.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payment.entity.OutboxEvent;
import com.payflow.payment.entity.OutboxStatus;
import com.payflow.payment.event.PaymentProcessedEvent;
import com.payflow.payment.kafka.PaymentEventProducer;
import com.payflow.payment.repository.OutboxEventRepository;
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
    private final PaymentEventProducer paymentEventProducer;
    private final ObjectMapper objectMapper;
    private final long publishTimeoutMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            PaymentEventProducer paymentEventProducer,
            ObjectMapper objectMapper,
            @Value("${app.outbox.publisher.timeout-ms:5000}") long publishTimeoutMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.paymentEventProducer = paymentEventProducer;
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

        log.debug("[Payment-Outbox] Discovered {} unpublished event(s)", pendingEvents.size());
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
        log.info("[Payment-Outbox] Discovered event to publish: id={}, aggregateId={}, eventType={}, status={}",
                outboxEvent.getId(), outboxEvent.getAggregateId(), outboxEvent.getEventType(), outboxEvent.getStatus());

        try {
            if ("PaymentProcessed".equalsIgnoreCase(outboxEvent.getEventType()) ||
                "PaymentSucceeded".equalsIgnoreCase(outboxEvent.getEventType()) ||
                "PaymentFailed".equalsIgnoreCase(outboxEvent.getEventType())) {

                PaymentProcessedEvent eventPayload = objectMapper.readValue(outboxEvent.getPayload(), PaymentProcessedEvent.class);
                log.info("[Payment-Outbox] Publishing event: id={}, orderId={}, sagaId={}, status={}",
                        outboxEvent.getId(), eventPayload.getOrderId(), eventPayload.getSagaId(), eventPayload.getStatus());

                CompletableFuture<SendResult<String, Object>> future = paymentEventProducer.sendPaymentProcessedEvent(eventPayload);
                SendResult<String, Object> sendResult = future.get(publishTimeoutMs, TimeUnit.MILLISECONDS);

                outboxEvent.setStatus(OutboxStatus.PUBLISHED);
                outboxEvent.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(outboxEvent);

                log.info("[Payment-Outbox] Publication succeeded: id={}, partition={}, offset={}",
                        outboxEvent.getId(),
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().partition() : -1,
                        sendResult != null && sendResult.getRecordMetadata() != null ? sendResult.getRecordMetadata().offset() : -1);
                return true;
            } else {
                log.warn("[Payment-Outbox] Unknown event type '{}' for outbox event id={}",
                        outboxEvent.getEventType(), outboxEvent.getId());
                return false;
            }
        } catch (Exception ex) {
            log.error("[Payment-Outbox] Publication failed for event id={} [aggregateId={}]: {}. Retrying on next run.",
                    outboxEvent.getId(), outboxEvent.getAggregateId(), ex.getMessage());
            // Record remains NEW, will be retried on next scheduled cycle
            return false;
        }
    }
}
