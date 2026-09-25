package com.shopmart.inventory.event;

import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-group")
    public void consumeOrderEvent(OrderEvent event) {
        log.info("InventoryService received event: {}", event);

        if (event.getType() == SagaEventType.ORDER_CREATED) {
            try {
                productService.decreaseStock(event.getProductId(), event.getQuantity());
                event.setType(SagaEventType.INVENTORY_RESERVED);
                log.info("Stock reserved for order id={}", event.getOrderId());
            } catch (Exception e) {
                log.error("Failed to reserve stock for order id={}: {}", event.getOrderId(), e.getMessage());
                event.setType(SagaEventType.INVENTORY_FAILED);
                event.setMessage(e.getMessage());
            }
            kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event);
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            // Compensating transaction
            try {
                productService.increaseStock(event.getProductId(), event.getQuantity());
                log.info("Compensated stock for order id={} after payment failed", event.getOrderId());
            } catch (Exception e) {
                log.error("Failed to compensate stock for order id={}: {}", event.getOrderId(), e.getMessage());
            }
        }
    }
}
