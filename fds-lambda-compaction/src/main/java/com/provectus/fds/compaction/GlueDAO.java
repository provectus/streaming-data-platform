package com.provectus.fds.compaction;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.*;
import com.provectus.fds.compaction.utils.GlueConverter;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GlueDAO {
    private final AWSGlueAsync client;
    private final String catalogId;


    private static AWSGlueAsync createAsyncGlueClient(int maxConnections)
    {
        ClientConfiguration clientConfig = new ClientConfiguration().withMaxConnections(maxConnections);
        AWSGlueAsyncClientBuilder asyncGlueClientBuilder = AWSGlueAsyncClientBuilder.standard()
                .withClientConfiguration(clientConfig);

        return asyncGlueClientBuilder.build();
    }


    public GlueDAO(String catalogId) {
        this.catalogId = catalogId;
        this.client = createAsyncGlueClient(1);
    }

    public void addPartition(String databaseName
            , String tableName
            , List<Map.Entry<String,String>> values
            , FileMetaData metaData) {

        GetTableResult getTableResult = client.getTable(new GetTableRequest()
                .withCatalogId(catalogId)
                .withDatabaseName(databaseName)
                .withName(tableName));

        List<String> partitionValues = values.stream().map(Map.Entry::getValue).collect(Collectors.toList());


        StorageDescriptor sd = getTableResult.getTable().getStorageDescriptor();

        StringBuilder sb = new StringBuilder();
        sb.append(sd.getLocation());
        if (!sd.getLocation().endsWith("/")) {
            sb.append("/");
        }

        for (Map.Entry<String,String> value : values) {
            sb.append(value.getKey());
            sb.append("=");
            sb.append(value.getValue());
            sb.append("/");
        }

        CreatePartitionRequest request = new CreatePartitionRequest();
        request.setCatalogId(catalogId);
        request.setDatabaseName(databaseName);
        request.setTableName(tableName);
        PartitionInput input = new PartitionInput();
        input.setValues(partitionValues);

        input.setStorageDescriptor(GlueConverter.convertStorage(metaData, sb.toString(), sd));

        try {
            GetPartitionResult partitionResult = client.getPartition(
                    new GetPartitionRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withTableName(tableName)
                            .withPartitionValues(partitionValues)
            );

            client.updatePartition(new UpdatePartitionRequest()
                    .withPartitionValueList(partitionValues)
                    .withPartitionInput(input)
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
            );
        } catch (EntityNotFoundException e){
            client.createPartition(new CreatePartitionRequest()
                    .withPartitionInput(input)
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
            );
        }
    }

}
