package com.payflow.order.kafka;

import com.payflow.order.command.OrderCommand;
import com.payflow.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandConsumer.class);

    private final OrderService orderService;

    public OrderCommandConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-command:order-command}",
            groupId = "${spring.kafka.consumer.group-id:order-service-group}"
    )
    public void consumeOrderCommand(OrderCommand command) {
        log.info("Received OrderCommand: type={}, commandId={}, sagaId={}, orderId={}, reason={}",
                command.getCommandType(), command.getCommandId(), command.getSagaId(), command.getOrderId(), command.getReason());

        try {
            if ("CONFIRM".equalsIgnoreCase(command.getCommandType())) {
                orderService.confirmOrder(command.getOrderId());
                log.info("Order #{} confirmed per Saga Orchestrator command", command.getOrderId());
            } else if ("CANCEL".equalsIgnoreCase(command.getCommandType())) {
                orderService.cancelOrder(command.getOrderId(), command.getReason());
                log.info("Order #{} cancelled per Saga Orchestrator command", command.getOrderId());
            } else {
                log.warn("Unknown OrderCommand type: {}", command.getCommandType());
            }
        } catch (Exception ex) {
            log.error("Error processing OrderCommand [sagaId={}, orderId={}]: {}",
                    command.getSagaId(), command.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
