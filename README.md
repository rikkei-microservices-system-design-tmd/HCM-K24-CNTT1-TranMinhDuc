SHOPMART - GIAO DICH PHAN TAN (SAGA PATTERN)

1. THONG TIN CHUNG
- Bai tap: Nang cap phan he dat hang thanh giao dich phan tan (Saga Pattern)
- Ho ten: TranMinhDuc
- Lop: HCM-K24-CNTT1
- URL Github: https://github.com/rikkei-microservices-system-design-tmd/HCM-K24-CNTT1-TranMinhDuc.git

2. CONG NGHE SU DUNG
- Java 17
- Spring Boot 3.3.5
- Spring Cloud 2023.0.3
- MySQL 8.x
- Apache Kafka & Zookeeper
- Redis

3. CAU TRUC DU AN
- config-repo: Thu muc chua cac file cau hinh chung cho he thong
- config-server (port 8888): May chu cau hinh tap trung (su dung profile native tro den config-repo)
- eureka-server (port 8761): May chu Service Registry va Discovery
- api-gateway (port 8080): Cong vao duy nhat, dinh tuyen cac request va load balancing
- order-service (port 8081): Quan ly don hang, FeignClient goi inventory-service va su dung Kafka phat/nhan su kien
- inventory-service (port 8082): Quan ly ton kho, Kafka consumer xu ly ton kho, Redis cache tang toc truy van
- payment-service (port 8083): Xu ly thanh toan, Kafka consumer lang nghe ket qua thanh toan

4. CAC YEU CAU DA HOAN THANH
A. CAU 1: HA TANG MICROSERVICE
- Da dung Config Server tai port 8888, cac service nap cau hinh tu do qua config-repo
- Da dung Eureka Server tai port 8761, cac service order, inventory, payment, gateway da duoc dang ky thanh cong
- Da dung API Gateway tai port 8080 voi cac tuyen duong /api/order/**, /api/inventory/**, /api/payment/** huong ve cac service tuong ung
- Tat ca ung dung duoc khai bao load balancer.

B. CAU 2: GIAO TIEP DONG BO & KHANG LOI
- Order-service su dung FeignClient (InventoryClient) de goi inventory-service lay thong tin san pham dong bo truoc khi tao don
- Tich hop CircuitBreaker cua Resilience4j trong qua trinh goi Feign de tranh hieu ung domino khi inventory-service ngung hoat dong, kem phuong thuc fallback "createOrderFallback".

C. CAU 3: SAGA PATTERN VA KAFKA
- Khai bao Kafka, Zookeeper trong docker-compose.yml
- Su dung mo hinh Choreography Saga thong qua topic "order":
  + Buoc 1: Order-service tao don hang PENDING, gui ORDER_CREATED
  + Buoc 2: Inventory-service nhan su kien, tru ton kho. Neu thanh cong gui INVENTORY_RESERVED, neu that bai gui INVENTORY_FAILED.
  + Buoc 3: Payment-service lang nghe INVENTORY_RESERVED, tien hanh thanh toan. Neu thanh cong gui PAYMENT_COMPLETED, that bai gui PAYMENT_FAILED.
  + Buoc 4: Order-service lang nghe PAYMENT_COMPLETED de doi trang thai COMPLETED, hoac nghe PAYMENT_FAILED/INVENTORY_FAILED de doi trang thai CANCELLED.
- Minh chung rollback: Neu thanh toan that bai, payment-service phat su kien PAYMENT_FAILED. Inventory-service nghe duoc, thuc hien cong lai ton kho (increaseStock) cho don hang tren. Order-service chuyen don hang sang CANCELLED.

D. CAU 4: REDIS CACHE
- Khai bao Redis trong docker-compose.yml
- Su dung Spring Cache cho inventory-service:
  + "Cacheable" tren getProductById de cache thong tin vao Redis
  + "CachePut" tren updateProduct, decreaseStock, increaseStock de cap nhat cache
  + "CacheEvict" tren deleteProduct de xoa cache.
- Lan dau truy van se co log ghi tu DB, cac lan sau khong con hien tuong do ma lay truc tiep tu Redis.

E. CAU 5: CLEAN CODE VA KIEM THU
- Da xoa bot cac comment chua noi dung thua thai trong cac class
- Xay dung unit test cho kich ban rollback trong OrderEventConsumerTest (order-service).
- Day du log INFO, ERROR tai cac diem can thiet
- Cau hinh khong hard-code ma duoc doc tu Config Server.
