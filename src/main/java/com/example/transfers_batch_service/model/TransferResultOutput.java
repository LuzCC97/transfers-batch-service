package com.example.transfers_batch_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferResultOutput {

    // Campos que vienen de la API de transfers
    private String transferId;
    private String status;
    private String transferType;
    private Double commissionApplied;

    // Campos originales del TXT (por si luego los quieres en el Parquet)
    private String customerId;
    private String sourceAccountId;
    private String destAccountId;
    private String currency;
    private Double amount;
    private String description;

    // Info de resultado
    private Boolean success;      // true = transferencia correcta, false = error
    private String errorCode;     // campo "error" del JSON de la API
    private String errorMessage;  // campo "message" del JSON de la API
}
