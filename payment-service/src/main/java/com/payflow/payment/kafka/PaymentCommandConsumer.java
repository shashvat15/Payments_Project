package com.payflow.payment.kafka;

import com.payflow.payment.command.ProcessPaymentCommand;
import com.payflow.payment.event.PaymentProcessedEvent;
import com.payflow.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandConsumer.class);

    private final PaymentService paymentService;
    private final PaymentEventProducer paymentEventProducer;

    public PaymentCommandConsumer(PaymentService paymentService, PaymentEventProducer paymentEventProducer) {
        this.paymentService = paymentService;
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-command:payment-command}",
            groupId = "${spring.kafka.consumer.group-id:payment-service-group}"
    )
    public void consumeProcessPaymentCommand(ProcessPaymentCommand command) {
        log.info("Received ProcessPaymentCommand: commandId={}, sagaId={}, orderId={}, amount={}, simulatePaymentFailure={}",
                command.getCommandId(), command.getSagaId(), command.getOrderId(), command.getAmount(), command.getSimulatePaymentFailure());

        try {
            PaymentProcessedEvent event = paymentService.processPaymentFromCommand(command);
            paymentEventProducer.sendPaymentProcessedEvent(event);
            log.info("Successfully processed payment command and published PaymentProcessedEvent for orderId={}, sagaId={}",
                    command.getOrderId(), command.getSagaId());
        } catch (Exception ex) {
            log.error("Failed to process payment command for orderId={}, sagaId={}: {}",
                    command.getOrderId(), command.getSagaId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
