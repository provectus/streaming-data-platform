package com.provectus.fds.compaction.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class JsonFileReader implements AutoCloseable,Iterable<GenericData.Record> {


    private final Schema schema;
    private final BufferedReader bufferedReader;


    public JsonFileReader(Schema schema, File file) throws IOException {
        this.schema = schema;
        this.bufferedReader = new BufferedReader(new FileReader(file));
    }

    @Override
    public void close() throws Exception {
        this.bufferedReader.close();
    }

    @Override
    public Iterator<GenericData.Record> iterator() {
        return new JsonFileReaderIterator(schema, bufferedReader);
    }

    private static class  JsonFileReaderIterator implements  Iterator<GenericData.Record> {
        private static final ObjectMapper mapper = new ObjectMapper();

        private final Schema schema;
        private final BufferedReader bufferedReader;
        private String currentLine;

        public JsonFileReaderIterator(Schema schema, BufferedReader bufferedReader) {
            this.schema = schema;
            this.bufferedReader = bufferedReader;
            try {
                this.currentLine=bufferedReader.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.currentLine!=null;
        }

        @Override
        public GenericData.Record next() {
            try {
                JsonNode jsonRecord = mapper.readTree(currentLine);
                GenericData.Record record =  (GenericData.Record) JsonUtils.convertToAvro(GenericData.get(), jsonRecord, schema);
                this.currentLine=this.bufferedReader.readLine();
                return record;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
