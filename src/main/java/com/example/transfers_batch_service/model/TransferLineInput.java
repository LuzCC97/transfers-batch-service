package com.example.transfers_batch_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferLineInput {
    private String originId;
    private String customerId;
    private String sourceAccountId;
    private String destAccountId;
    private String currency;
    private Double amount;
    private String description;
}
