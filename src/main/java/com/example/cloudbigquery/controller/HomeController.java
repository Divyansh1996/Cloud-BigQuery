package com.example.cloudbigquery.controller;

import com.example.cloudbigquery.model.User;
import com.google.cloud.bigquery.*;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import com.google.cloud.spring.bigquery.core.WriteApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
public class HomeController {

    @Autowired
    BigQuery bigQuery;

    @Autowired
    BigQueryTemplate bigQueryTemplate;


    @PostMapping("/insertByCsv")
    public String insertByCsv(@RequestParam(name = "file")MultipartFile file) throws IOException, ExecutionException, InterruptedException {
        String tableName = "newTable";
        CompletableFuture<Job> jobCompletableFuture = bigQueryTemplate.writeDataToTable(tableName, file.getInputStream(), FormatOptions.csv());
        Job job = jobCompletableFuture.get();
        if(job.isDone()){
            return "Job Done successfully";
        }
        return "Job Failed";
    }

    @PostMapping("/insertByData")
    public String insertByData(@RequestBody User user) throws InterruptedException {
        String query = "insert into `newDataset.newTable` values("+user.getId()+",'"+user.getName()+"','"+user.getEmail()+"')";
        System.out.println("query: " + query);
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        TableResult tableResults = bigQuery.query(queryJobConfiguration);
        return "Data Inserted Successfully";
    }

    @PostMapping("/insertByJson")
    public String insertByJson(@RequestParam(name = "file")MultipartFile file) throws InterruptedException, IOException, ExecutionException {
        CompletableFuture<WriteApiResponse> writeApiResponses = bigQueryTemplate.writeJsonStream("newTable", file.getInputStream());
        WriteApiResponse writeApiResponse = writeApiResponses.get();
        if(writeApiResponse.isSuccessful()){
            return "Data Inserted Successfully";
        }
        return "Data Insertion Failed";
    }

    @GetMapping("/getData/{tableName}/{id}")
    public User getData(@PathVariable("tableName") String tableName, @PathVariable("id") int id) throws InterruptedException {
        String query = "select * from `newDataset."+tableName+"` where id=\""+id+"\"";
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        TableResult tableResults = bigQuery.query(queryJobConfiguration);
        User user = new User();
        for(FieldValueList row: tableResults.iterateAll()){
            user.setEmail(row.get("email").getStringValue());
            user.setId(Integer.parseInt(row.get("id").getStringValue()));
            user.setName(row.get("name").getStringValue());
        }
        return user;
    }

    @DeleteMapping("/deleteData/{id}")
    public String deleteData(@PathVariable("id") String id) throws InterruptedException {
        String query = "delete from `newDataset.newTable` where id=\""+id+"\"";
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        TableResult tableResult = bigQuery.query(queryJobConfiguration);
        return "Data Deleted Successfully";
    }

    public Schema getSchema(){
        return Schema.of(Field.of("Id",StandardSQLTypeName.STRING),
        Field.of("Name",StandardSQLTypeName.STRING), Field.of("Email",StandardSQLTypeName.STRING));
    }
}
