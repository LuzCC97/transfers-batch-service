package com.example.transfers_batch_service.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransferResultOutputTest {

    //Esta clase prueba la clase TransferResultOutput que representa el resultado de una operación de transferencia.
    @Test
    void testTransferResultOutputGettersAndSetters() {
        // Arrange
        TransferResultOutput output = new TransferResultOutput();

        // Act
        output.setTransferId("TXN123");
        output.setStatus("COMPLETED");
        output.setTransferType("INTERNAL");
        output.setCommissionApplied(5.0);
        output.setCustomerId("CUST123");
        output.setSourceAccountId("ACC123");
        output.setDestAccountId("ACC456");
        output.setCurrency("USD");
        output.setAmount(1000.0);
        output.setDescription("Test transfer");
        output.setSuccess(true);
        output.setErrorCode(null);
        output.setErrorMessage(null);

        // Assert
        assertEquals("TXN123", output.getTransferId());
        assertEquals("COMPLETED", output.getStatus());
        assertTrue(output.getSuccess());
        assertNull(output.getErrorCode());
    }


}