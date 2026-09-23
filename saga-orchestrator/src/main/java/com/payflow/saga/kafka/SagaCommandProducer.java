package com.payflow.saga.kafka;

import com.payflow.saga.command.InventoryCommand;
import com.payflow.saga.command.OrderCommand;
import com.payflow.saga.command.ProcessPaymentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SagaCommandProducer {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String inventoryCommandTopic;
    private final String paymentCommandTopic;
    private final String orderCommandTopic;

    public SagaCommandProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.inventory-command:inventory-command}") String inventoryCommandTopic,
            @Value("${app.kafka.topics.payment-command:payment-command}") String paymentCommandTopic,
            @Value("${app.kafka.topics.order-command:order-command}") String orderCommandTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryCommandTopic = inventoryCommandTopic;
        this.paymentCommandTopic = paymentCommandTopic;
        this.orderCommandTopic = orderCommandTopic;
    }

    public void sendInventoryCommand(InventoryCommand command) {
        String key = String.valueOf(command.getOrderId());
        log.info("[Saga: {}] Sending InventoryCommand [{}] to topic '{}' with key '{}'",
                command.getSagaId(), command.getCommandType(), inventoryCommandTopic, key);
        kafkaTemplate.send(inventoryCommandTopic, key, command);
    }

    public void sendProcessPaymentCommand(ProcessPaymentCommand command) {
        String key = String.valueOf(command.getOrderId());
        log.info("[Saga: {}] Sending ProcessPaymentCommand to topic '{}' with key '{}': amount={}",
                command.getSagaId(), paymentCommandTopic, key, command.getAmount());
        kafkaTemplate.send(paymentCommandTopic, key, command);
    }

    public void sendOrderCommand(OrderCommand command) {
        String key = String.valueOf(command.getOrderId());
        log.info("[Saga: {}] Sending OrderCommand [{}] to topic '{}' with key '{}'",
                command.getSagaId(), command.getCommandType(), orderCommandTopic, key);
        kafkaTemplate.send(orderCommandTopic, key, command);
    }
}
