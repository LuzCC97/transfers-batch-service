package com.example.transfers_batch_service.config;

import com.example.transfers_batch_service.model.TransferLineInput;
import com.example.transfers_batch_service.model.TransferResultOutput;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private static final String TRANSFER_RESULT_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"TransferResultOutput\","
            + "\"namespace\":\"com.example.transfersbatch.parquet\","
            + "\"fields\":["
            + "  {\"name\":\"transferId\",      \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"status\",          \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"transferType\",    \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"commissionApplied\",\"type\":[\"null\",\"double\"], \"default\":null},"
            + "  {\"name\":\"customerId\",      \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"sourceAccountId\", \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"destAccountId\",   \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"currency\",        \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"amount\",          \"type\":[\"null\",\"double\"], \"default\":null},"
            + "  {\"name\":\"description\",     \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"success\",         \"type\":[\"null\",\"boolean\"],\"default\":null},"
            + "  {\"name\":\"errorCode\",       \"type\":[\"null\",\"string\"], \"default\":null},"
            + "  {\"name\":\"errorMessage\",    \"type\":[\"null\",\"string\"], \"default\":null}"
            + "]}";


    // Spring Boot / Spring Batch 5 te inyecta estos beans
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    //Bean para definir un RestTemplate para llamar al API transfers-service
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public Step demoStep() {
        return new StepBuilder("demoStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(">>> Ejecutando job de ejemplo de TRANSFERS BATCH <<<");
                    return RepeatStatus.FINISHED; // indica que ya terminó
                }, transactionManager)
                .build();
    }

    @Bean
    public Job demoJob() {
        return new JobBuilder("demoJob", jobRepository)
                .start(demoStep())
                .build();
    }
    @Bean
    public FlatFileItemReader<TransferLineInput> transferFileReader() {
        FlatFileItemReader<TransferLineInput> reader = new FlatFileItemReader<>();

        // 1) De dónde lee el archivo
        reader.setResource(new ClassPathResource("input-transfers.txt"));

        // 2) Cómo se mapea cada línea a un objeto
        DefaultLineMapper<TransferLineInput> lineMapper = new DefaultLineMapper<>();

        // Tokenizer: separa por | y asigna nombres de campos
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter("|");
        tokenizer.setNames("customerId", "sourceAccountId", "destAccountId",
                "currency", "amount", "description");

        // FieldSetMapper: construye el objeto TransferLineInput
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            TransferLineInput item = new TransferLineInput();
            item.setCustomerId(fieldSet.readString("customerId"));
            item.setSourceAccountId(fieldSet.readString("sourceAccountId"));
            item.setDestAccountId(fieldSet.readString("destAccountId"));
            item.setCurrency(fieldSet.readString("currency"));
            item.setAmount(fieldSet.readDouble("amount"));
            item.setDescription(fieldSet.readString("description"));
            return item;
        });

        reader.setLineMapper(lineMapper);
        return reader;
    }

    //Transforma cada TransferLineInput en un TransferResultOutput. Aqui tengo la logica del negocio (validaciones, calculos)
    @Bean
    public ItemProcessor<TransferLineInput, TransferResultOutput> transferProcessor(RestTemplate restTemplate) {

        ObjectMapper objectMapper = new ObjectMapper(); // para leer el JSON de error

        return line -> {
            // 1) Armamos el body que espera mi API
            Map<String, Object> body = Map.of(
                    "customer", Map.of("customerId", line.getCustomerId()),
                    "sourceAccount", Map.of("accountId", line.getSourceAccountId()),
                    "destinationAccount", Map.of("accountId", line.getDestAccountId()),
                    "transferData", Map.of(
                            "currency", line.getCurrency(),
                            "amount", line.getAmount(),
                            "description", line.getDescription()
                    )
            );

            String url = "http://localhost:8081/transfers/create";

            TransferResultOutput out = new TransferResultOutput();

            try {
                // 2)  llamada a la API
                @SuppressWarnings("unchecked")
                Map<String, Object> response = restTemplate.postForObject(url, body, Map.class);

                System.out.println("Respuesta OK API transfers para línea " + line.getCustomerId() + ": " + response);

                if (response != null) {
                    out.setTransferId((String) response.get("transferId"));
                    out.setStatus((String) response.get("status"));
                    out.setTransferType((String) response.get("transferType"));

                    Object commission = response.get("commissionApplied");
                    if (commission != null) {
                        out.setCommissionApplied(Double.valueOf(commission.toString()));
                    }
                }

                out.setSuccess(true);      // ✅ fue exitoso
                out.setErrorCode(null);
                out.setErrorMessage(null);

            } catch (HttpStatusCodeException e) {
                // 3) Aquí entramos si la API devolvió 4xx o 5xx

                String responseBody = e.getResponseBodyAsString();
                System.out.println("Error al llamar API transfers para línea " + line.getCustomerId()
                        + " -> status: " + e.getStatusCode() + ", body: " + responseBody);

                // Tratamos de leer el JSON de error: {"error":"...", "message":"..."}
                String errorCode = null;
                String errorMessage = null;
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> errorMap = objectMapper.readValue(responseBody, Map.class);
                    errorCode = (String) errorMap.get("error");
                    errorMessage = (String) errorMap.get("message");
                } catch (Exception ex) {
                    // Si por alguna razón no se puede parsear, guardamos todo el body en errorMessage
                    errorMessage = responseBody;
                }

                out.setSuccess(false);
                out.setErrorCode(errorCode);
                out.setErrorMessage(errorMessage);

                // Como hubo error, no hay transferId ni status ni commission
                out.setTransferId(null);
                out.setStatus(null);
                out.setTransferType(null);
                out.setCommissionApplied(null);
            }

            // 4) Copiamos SIEMPRE los datos de entrada del TXT
            out.setCustomerId(line.getCustomerId());
            out.setSourceAccountId(line.getSourceAccountId());
            out.setDestAccountId(line.getDestAccountId());
            out.setCurrency(line.getCurrency());
            out.setAmount(line.getAmount());
            out.setDescription(line.getDescription());

            return out;
        };
    }

    //Escribe los TransferResultOutput (datos procesados) en un archivo Parquet
    @Bean
    public ItemWriter<TransferResultOutput> transferParquetWriter(
            @Value("${app.output.parquet-file}") String outputFile) {

        return items -> {
            try {
                // 1) Get absolute file path
                File file = new File(outputFile).getAbsoluteFile();
                String filePath = file.getAbsolutePath();

                // 2) Create directory if it doesn't exist
                File parentDir = file.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    boolean dirsCreated = parentDir.mkdirs();
                    System.out.println("Directorio creado: " + dirsCreated + " - " + parentDir.getAbsolutePath());
                }

                System.out.println("Intentando escribir archivo en: " + filePath);

                if (items == null || !items.iterator().hasNext()) {
                    System.out.println("No hay elementos para escribir en el archivo Parquet");
                    return;
                }

                // 3) Hadoop configuration - minimal configuration
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
                conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                conf.set("fs.defaultFS", "file:///");

                // 4) Create Path using the file path directly
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);

                // 5) Create schema
                Schema schema = new Schema.Parser().parse(TRANSFER_RESULT_SCHEMA_JSON);

                // 6) Implementa la interfaz OutputFile de Parquet. Maneja la escritura de archivos en el sistema de archivos local
                //Use our custom LocalOutputFile para manejar la escritura en Windows.  para evitar problemas con las rutas de Windows
                LocalOutputFile localOutputFile = LocalOutputFile.fromPath(path, conf);
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                        .<GenericRecord>builder(localOutputFile)
                        .withSchema(schema)
                        .withConf(conf)
                        .build()) {

                    // 7) Write each record
                    for (TransferResultOutput item : items) {
                        GenericRecord record = new GenericData.Record(schema);
                        // Map your TransferResultOutput to GenericRecord
                        record.put("transferId", item.getTransferId());
                        record.put("status", item.getStatus());
                        record.put("transferType", item.getTransferType());
                        // ... add all other fields from TransferResultOutput to the record

                        writer.write(record);
                    }
                    System.out.println("✅ Archivo Parquet generado en: " + filePath);
                }
            } catch (Exception e) {
                System.err.println("Error al escribir el archivo Parquet: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Error al escribir el archivo Parquet", e);
            }
        };
    }

    @Bean
    public Step importTransfersStep(FlatFileItemReader<TransferLineInput> transferFileReader,
                                    ItemProcessor<TransferLineInput, TransferResultOutput> transferProcessor,
                                    ItemWriter<TransferResultOutput> transferParquetWriter) {

        System.out.println("===== CONFIGURANDO importTransfersStep =====");
        System.out.println("Reader: " + (transferFileReader != null ? "OK" : "NULL"));
        System.out.println("Processor: " + (transferProcessor != null ? "OK" : "NULL"));
        System.out.println("Writer: " + (transferParquetWriter != null ? "OK" : "NULL"));

        return new StepBuilder("importTransfersStep", jobRepository)
                .<TransferLineInput, TransferResultOutput>chunk(10, transactionManager) // Reducido para depuración
                .reader(transferFileReader)
                .processor(transferProcessor)
                .writer(transferParquetWriter)
                .listener(new ItemReadListener<TransferLineInput>() {
                    @Override
                    public void beforeRead() {
                        // No es necesario implementar
                    }

                    @Override
                    public void afterRead(TransferLineInput item) {
                        System.out.println("Leyendo línea: " + item);
                    }

                    @Override
                    public void onReadError(Exception ex) {
                        System.err.println("Error al leer: " + ex.getMessage());
                    }
                })
                .allowStartIfComplete(true)
                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .build();
    }


    @Bean
    public Job importTransfersJob(Step importTransfersStep) {
        return new JobBuilder("importTransfersJob", jobRepository)
                .incrementer(new RunIdIncrementer())  // 👈 clave
                .start(importTransfersStep)
                .build();
    }

}
