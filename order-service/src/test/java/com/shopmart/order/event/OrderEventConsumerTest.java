package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock
    private OrderService orderService;

    @InjectMocks
    private OrderEventConsumer orderEventConsumer;

    @Test
    void consumeOrderEvent_whenPaymentFailed_shouldCancelOrder() {
        OrderEvent event = new OrderEvent(1L, 2L, 1, BigDecimal.valueOf(100), SagaEventType.PAYMENT_FAILED, "Insufficient balance");

        orderEventConsumer.consumeOrderEvent(event);

        verify(orderService).cancelOrder(1L, "Insufficient balance");
    }
}
