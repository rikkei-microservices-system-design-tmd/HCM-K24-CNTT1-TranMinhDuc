package com.shopmart.payment.event;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-group")
    public void consumeOrderEvent(OrderEvent event) {
        log.info("PaymentService received event: {}", event);

        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            try {
                PaymentRequest request = new PaymentRequest(event.getOrderId(), event.getAmount());
                PaymentResponse response = paymentService.processPayment(request);

                if (response.getStatus() == PaymentStatus.SUCCESS) {
                    event.setType(SagaEventType.PAYMENT_PROCESSED);
                    log.info("Payment SUCCESS for order id={}", event.getOrderId());
                } else {
                    event.setType(SagaEventType.PAYMENT_FAILED);
                    event.setMessage(response.getMessage());
                    log.error("Payment FAILED for order id={}: {}", event.getOrderId(), response.getMessage());
                }
            } catch (Exception e) {
                log.error("Error processing payment for order id={}: {}", event.getOrderId(), e.getMessage());
                event.setType(SagaEventType.PAYMENT_FAILED);
                event.setMessage(e.getMessage());
            }
            kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event);
        }
    }
}
