package com.payflow.inventory.kafka;

import com.payflow.inventory.command.InventoryCommand;
import com.payflow.inventory.event.InventoryResultEvent;
import com.payflow.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandConsumer.class);

    private final InventoryService inventoryService;
    private final InventoryEventProducer inventoryEventProducer;

    public InventoryCommandConsumer(InventoryService inventoryService, InventoryEventProducer inventoryEventProducer) {
        this.inventoryService = inventoryService;
        this.inventoryEventProducer = inventoryEventProducer;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-command:inventory-command}",
            groupId = "${spring.kafka.consumer.group-id:inventory-service-group}"
    )
    public void consumeInventoryCommand(InventoryCommand command) {
        log.info("Received InventoryCommand: type={}, commandId={}, sagaId={}, orderId={}, productId={}, quantity={}",
                command.getCommandType(), command.getCommandId(), command.getSagaId(), command.getOrderId(),
                command.getProductId(), command.getQuantity());

        try {
            InventoryResultEvent resultEvent;

            if ("RELEASE".equalsIgnoreCase(command.getCommandType())) {
                resultEvent = inventoryService.releaseInventory(
                        command.getSagaId(),
                        command.getOrderId(),
                        command.getProductId(),
                        command.getQuantity()
                );
            } else {
                // Default to RESERVE
                resultEvent = inventoryService.reserveInventory(
                        command.getSagaId(),
                        command.getOrderId(),
                        command.getProductId(),
                        command.getQuantity(),
                        command.getSimulateInventoryFailure()
                );
            }

            inventoryEventProducer.sendInventoryResultEvent(resultEvent);
            log.info("Processed InventoryCommand and published result: sagaId={}, status={}",
                    command.getSagaId(), resultEvent.getStatus());

        } catch (Exception ex) {
            log.error("Error processing InventoryCommand [sagaId={}]: {}", command.getSagaId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
