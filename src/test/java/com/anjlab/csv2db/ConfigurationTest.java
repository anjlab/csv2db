package com.anjlab.csv2db;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.anjlab.csv2db.Configuration.CSVOptions;
import com.anjlab.csv2db.Configuration.OperationMode;

public class ConfigurationTest
{
    @Test
    public void createTestConfig() throws Exception
    {
        Configuration config = new Configuration();

        config.setForceUpdate(true);

        Map<Integer, String> columnMappings = new HashMap<Integer, String>();
        columnMappings.put(0, "company_name");
        columnMappings.put(1, "company_number");
        columnMappings.put(4, "address_line_1");
        columnMappings.put(5, "address_line_2");
        columnMappings.put(12, "country_of_origin");

        config.setColumnMappings(columnMappings);

        config.setTransientColumns(Arrays.asList("country_of_origin"));

        Map<String, ValueDefinition> connectionProperties = new HashMap<String, ValueDefinition>();
        connectionProperties.put("username", new StringLiteral("sa"));
        connectionProperties.put("password", new StringLiteral(""));

        config.setConnectionProperties(connectionProperties);

        config.setConnectionUrl("jdbc:derby:memory:myDB;create=true");
        config.setDriverClass("org.apache.derby.jdbc.EmbeddedDriver");

        CSVOptions csvOptions = new CSVOptions();
        csvOptions.setSkipLines(1);
        csvOptions.setEscapeChar('\b');
        config.setCsvOptions(csvOptions);
        config.setOperationMode(OperationMode.MERGE);
        config.setPrimaryKeys(Arrays.asList("company_number"));
        config.setTargetTable("companies_house_records");

        Map<String, ValueDefinition> insertValues = new HashMap<String, ValueDefinition>();
        insertValues.put("id", new SqlLiteral("current_timestamp"));
        config.setInsertValues(insertValues);

        Map<String, ValueDefinition> updateValues = new HashMap<String, ValueDefinition>();
        updateValues.put("updated_at", new SqlLiteral("current_date"));
        config.setUpdateValues(updateValues);

        String expectedJson = config.toJson();

        String actualJson = Configuration.fromJson("src/test/resources/test-config.json").toJson();

        Assert.assertEquals(expectedJson, actualJson);
    }

    @Test
    public void testExtend() throws FileNotFoundException, ConfigurationException
    {
        Configuration config = Configuration.fromJson(
                "src/test/resources/test-config-extended.json");

        // Test override simple property
        Assert.assertEquals("jdbc:derby:memory:myDB;create=true",
                config.getConnectionUrl());

        // Test override just one key on a nested object
        Assert.assertEquals("sa", config.getConnectionProperties().get("username"));
        Assert.assertEquals("secret", config.getConnectionProperties().get("password"));

        // Test override connection property with dynamic value
        Assert.assertEquals(
                System.getProperty("user.name") + ".custom-from-js",
                config.getConnectionProperties().get("custom"));
    }
}
