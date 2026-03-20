package com.tbdhealth.parquetdemo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderRecord {

    private String orderId;
    private String email;
    private String region;               // from ShippingAddress.state
    private String product;              // from LineItems.productName
    private int quantity;
    private double unitPrice;
    private String orderStatus;          // OrderStatus enum → string
    private String fulfillmentStatus;    // FulfillmentStatus enum → string
    private String storeSourceType;      // StoreSourceType enum → string
    private String orderCreateDate;      // ZonedDateTime → ISO string
    private String vendor;
    private String kitId;
    private String outboundTrackingNumber;
    private String inboundTrackingNumber;
}
