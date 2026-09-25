package com.payflow.payment.kafka;

import com.payflow.payment.command.ProcessPaymentCommand;
import com.payflow.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandConsumer.class);

    private final PaymentService paymentService;

    public PaymentCommandConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-command:payment-command}",
            groupId = "${spring.kafka.consumer.group-id:payment-service-group}"
    )
    public void consumeProcessPaymentCommand(ProcessPaymentCommand command) {
        log.info("Received ProcessPaymentCommand: commandId={}, sagaId={}, orderId={}, amount={}, simulatePaymentFailure={}",
                command.getCommandId(), command.getSagaId(), command.getOrderId(), command.getAmount(), command.getSimulatePaymentFailure());

        try {
            paymentService.processPaymentFromCommand(command);
            log.info("Successfully processed payment command into payment_db and outbox for orderId={}, sagaId={}",
                    command.getOrderId(), command.getSagaId());
        } catch (Exception ex) {
            log.error("Failed to process payment command for orderId={}, sagaId={}: {}",
                    command.getOrderId(), command.getSagaId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
