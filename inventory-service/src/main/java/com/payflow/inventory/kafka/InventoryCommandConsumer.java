package com.payflow.inventory.kafka;

import com.payflow.inventory.command.InventoryCommand;
import com.payflow.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandConsumer.class);

    private final InventoryService inventoryService;

    public InventoryCommandConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
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
            if ("RELEASE".equalsIgnoreCase(command.getCommandType())) {
                inventoryService.releaseInventory(
                        command.getSagaId(),
                        command.getOrderId(),
                        command.getProductId(),
                        command.getQuantity()
                );
            } else {
                // Default to RESERVE
                inventoryService.reserveInventory(
                        command.getSagaId(),
                        command.getOrderId(),
                        command.getProductId(),
                        command.getQuantity(),
                        command.getSimulateInventoryFailure()
                );
            }

            log.info("Processed InventoryCommand into inventory_db and outbox for sagaId={}, orderId={}",
                    command.getSagaId(), command.getOrderId());

        } catch (Exception ex) {
            log.error("Error processing InventoryCommand [sagaId={}]: {}", command.getSagaId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
