package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-group")
    public void consumeOrderEvent(OrderEvent event) {
        log.info("OrderService received event: {}", event);

        try {
            if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
                orderService.completeOrder(event.getOrderId());
                log.info("Order id={} marked as COMPLETED", event.getOrderId());
            } else if (event.getType() == SagaEventType.PAYMENT_FAILED || event.getType() == SagaEventType.INVENTORY_FAILED) {
                orderService.cancelOrder(event.getOrderId(), event.getMessage());
                log.info("Order id={} marked as CANCELLED due to {}", event.getOrderId(), event.getMessage());
            }
        } catch (Exception e) {
            log.error("Error processing event for order id={}: {}", event.getOrderId(), e.getMessage());
        }
    }
}
