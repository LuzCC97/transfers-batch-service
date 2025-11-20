package com.example.transfers_batch_service.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

//Esta clase prueba la clase
//TransferLineInput que es un modelo de datos simple que representa una línea de entrada para una transferencia.
class TransferLineInputTest {

    //Este metodo prueba los getters y setters de la clase TransferLineInput
    @Test
    void testTransferLineInputGettersAndSetters() {
        // Arrange
        TransferLineInput input = new TransferLineInput();

        // Act
        input.setCustomerId("CUST123");
        input.setSourceAccountId("ACC123");
        input.setDestAccountId("ACC456");
        input.setCurrency("USD");
        input.setAmount(1000.0);
        input.setDescription("Test transfer");

        // Assert
        assertEquals("CUST123", input.getCustomerId());
        assertEquals("ACC123", input.getSourceAccountId());
        assertEquals("ACC456", input.getDestAccountId());
        assertEquals("USD", input.getCurrency());
        assertEquals(1000.0, input.getAmount());
        assertEquals("Test transfer", input.getDescription());
    }

    //Este metodo prueba el constructor con todos los argumentos:
    @Test
    void testTransferLineInputAllArgsConstructor() {
        // Arrange & Act
        TransferLineInput input = new TransferLineInput(
                "CUST123", "ACC123", "ACC456", "USD", 1000.0, "Test transfer"
        );

        // Assert
        assertNotNull(input);
        assertEquals("CUST123", input.getCustomerId());
        assertEquals(1000.0, input.getAmount());
    }
}