package com.gigaspaces.jdbc.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;


@SpaceClass
public class Purchase {
    private String id;
    private Integer amount;
    private String customerId;
    private String productId;
    private String inventoryId;


    public Purchase() {
    }

    public Purchase(Integer amount, String customerId, String productId, String inventoryId) {
        this.amount = amount;
        this.customerId = customerId;
        this.productId = productId;
        this.inventoryId = inventoryId;
    }

    @SpaceId (autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getInventoryId() {
        return inventoryId;
    }

    public void setInventoryId(String inventoryId) {
        this.inventoryId = inventoryId;
    }
}
