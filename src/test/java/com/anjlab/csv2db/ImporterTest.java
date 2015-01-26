package com.anjlab.csv2db;

import com.anjlab.csv2db.Configuration.CSVOptions;
import com.anjlab.csv2db.Configuration.OperationMode;
import junit.framework.Assert;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImporterTest
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

        Map<String, String> connectionProperties = new HashMap<String, String>();
        connectionProperties.put("username", "sa");
        connectionProperties.put("password", "");

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
    public void testImport() throws Exception
    {
        Configuration config = Configuration.fromJson(
                "src/test/resources/test-config.json");

        config.getCsvOptions().setEscapeChar((char) 0);

        Importer importer = new Importer(config, 1, new SimpleFileResolver("src/test/resources"));

        Connection connection = importer.createConnection();

        connection.createStatement()
                .executeUpdate(
                        "create table companies_house_records (" +
                                "id timestamp not null," +
                                "company_name varchar(160)," +
                                "company_number varchar(8)," +
                                "address_line_1 varchar(300)," +
                                "address_line_2 varchar(300)," +
                                "updated_at date" +
                                ")");

        importer.performImport("src/test/resources/test-data.csv");

        //  For the first time the data will be inserted, we don't insert any values to updated_at in this test case
        assertRecordCount(connection, getExpectedDataset(false), true);

        importer.performImport("src/test/resources/test-data.csv");

        //  Data will be updated and we should have updated_at set
        assertRecordCount(connection, getExpectedDataset(true), true);

        connection.close();

        config.setOperationMode(OperationMode.INSERT);
        config.setBatchSize(3);

        importer = new Importer(config, 1, new SimpleFileResolver("src/test/resources"));

        connection = importer.createConnection();

        importer.performImport("src/test/resources/test-data.csv");

        List<Object[]> expectedDataset = new ArrayList<Object[]>();
        expectedDataset.addAll(getExpectedDataset(true));
        expectedDataset.addAll(getExpectedDataset(false));
        sortDatasetByCompanyNameAndUpdateDate(expectedDataset);
        assertRecordCount(connection, expectedDataset, true);

        //  Test import from ZIP

        importer.performImport("src/test/resources/test-data.zip");

        expectedDataset.addAll(getExpectedDataset(false));
        sortDatasetByCompanyNameAndUpdateDate(expectedDataset);
        assertRecordCount(connection, expectedDataset, true);

        // Test InsertOnly

        connection.close();

        config.setOperationMode(OperationMode.INSERTONLY);
        config.setBatchSize(3);

        importer = new Importer(config, 1, new SimpleFileResolver("src/test/resources"));

        connection = importer.createConnection();

        importer.performImport("src/test/resources/test-data.csv");

        assertRecordCount(connection, expectedDataset, true);

        connection.createStatement()
                .executeUpdate("delete from companies_house_records");

        config.setIgnoreNullPK(true);

        importer.performImport("src/test/resources/test-data.csv");

        // For the first time the data will be inserted, we don't insert any values to updated_at in
        // this test case
        assertRecordCount(connection, getExpectedDataset(false), true);
    }

    @Test
    public void testMultithreadImport() throws Exception
    {
        Configuration config = Configuration.fromJson(
                "src/test/resources/test-config.json");

        config.getCsvOptions().setEscapeChar((char) 0);

        Importer importer = new Importer(config, 4, new SimpleFileResolver("src/test/resources"));

        Connection connection = importer.createConnection();

        connection.createStatement()
                .executeUpdate("drop table companies_house_records");

        connection.createStatement()
                .executeUpdate(
                        "create table companies_house_records (" +
                                "id timestamp not null," +
                                "company_name varchar(160)," +
                                "company_number varchar(8)," +
                                "address_line_1 varchar(300)," +
                                "address_line_2 varchar(300)," +
                                "updated_at date" +
                                ")");

        importer.performImport("src/test/resources/test-data.csv");

        //  For the first time the data will be inserted, we don't insert any values to updated_at in this test case
        assertRecordCount(connection, getExpectedDataset(false), true);

        importer.performImport("src/test/resources/test-data.csv");

        //  Data will be updated and we should have updated_at set
        assertRecordCount(connection, getExpectedDataset(true), true);

        connection.close();

        config.setOperationMode(OperationMode.INSERT);
        config.setBatchSize(3);

        importer = new Importer(config, 4, new SimpleFileResolver("src/test/resources"));

        connection = importer.createConnection();

        importer.performImport("src/test/resources/test-data.csv");

        List<Object[]> expectedDataset = new ArrayList<Object[]>();
        expectedDataset.addAll(getExpectedDataset(true));
        expectedDataset.addAll(getExpectedDataset(false));
        sortDatasetByCompanyNameAndUpdateDate(expectedDataset);
        assertRecordCount(connection, expectedDataset, true);

        //  Test import from ZIP

        importer.performImport("src/test/resources/test-data.zip");

        expectedDataset.addAll(getExpectedDataset(false));
        sortDatasetByCompanyNameAndUpdateDate(expectedDataset);
        assertRecordCount(connection, expectedDataset, true);
    }

    private List<Object[]> getExpectedDataset(boolean withDate)
    {
        Calendar cal = Calendar.getInstance();
        cal.clear(Calendar.HOUR);
        cal.clear(Calendar.MINUTE);
        cal.clear(Calendar.SECOND);
        cal.clear(Calendar.MILLISECOND);

        Date today = withDate ? new Date(cal.getTimeInMillis()) : null;

        return Arrays.asList(
                new Object[]{"! LTD", "08209948", "METROHOUSE 57 PEPPER ROAD", "HUNSLET", today},
                new Object[]{"!BIG IMPACT GRAPHICS LIMITED", "07382019", "335 ROSDEN HOUSE", "372 OLD STREET", today},
                new Object[]{"!NFERNO LTD.", "04753368", "FIRST FLOOR THAVIES INN HOUSE 3-4", "HOLBORN CIRCUS", today},
                new Object[]{"!NSPIRED LTD", "SC421617", "12 BON ACCORD SQUARE", "", today},
                new Object[]{"!OBAC INSTALLATIONS LIMITED", "07527820", "DEVONSHIRE HOUSE", "60 GOSWELL ROAD", today},
                new Object[]{"!OBAC UK LIMITED", "07687209", "DEVONSHIRE HOUSE", "60 GOSWELL ROAD", today},
                new Object[]{"!ST MEDIA SOUTHAMPTON LTD", "07904170", "10 NORTHBROOK HOUSE", "FREE STREET, BISHOPS WALTHAM", today},
                new Object[]{"ALJOH B.V.", "SF000899", "ALEXANDER HINSHELWOOD BARR", "\"SHALIMAR\"", today},
                new Object[]{"ALLEGIS SERVICES (INDIA) PRIVATE LIMITED", "FC027847", "\\54, 1ST MAIN ROAD", "3RD PHASE", today},
                new Object[]{"APS DIRECT LIMITED", "05638208", "MANOR COURT CHAMBERS \\", "126 MANOR COURT ROAD", today});
    }

    protected void assertRecordCount(Connection connection, List<Object[]> expectedData, boolean queryWithUpdateDate) throws SQLException
    {
        String query;
        if (queryWithUpdateDate)
        {
            query = "SELECT * FROM companies_house_records ORDER BY company_name, updated_at";
        }
        else
        {
            query = "SELECT * FROM companies_house_records ORDER BY company_name";
        }
        ResultSet resultSet;
        resultSet = connection.createStatement()
                .executeQuery(query);

        int index = 0;
        while (resultSet.next())
        {
            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int i = 2; i <= columnCount; i++)
            {
                Object columnValue = resultSet.getObject(i);

                if (columnValue != null && columnValue instanceof Date)
                {
                    Assert.assertEquals(expectedData.get(index)[i - 2].toString(), columnValue.toString());
                }
                else
                {
                    Assert.assertEquals(expectedData.get(index)[i - 2], columnValue);
                }
            }
            index++;
        }
        resultSet.close();
    }

    @Test
    public void testImportWithScripting() throws Exception
    {
        Configuration config = Configuration.fromJson(
                "src/test/resources/test-config-with-scripting.json");

        config.getCsvOptions().setEscapeChar((char) 0);

        Importer importer = new Importer(config, 1, new SimpleFileResolver("src/test/resources"));

        Connection connection = importer.createConnection();

        connection.createStatement()
                .executeUpdate("drop table companies_house_records");

        connection.createStatement()
                .executeUpdate(
                        "create table companies_house_records (" +
                                "id timestamp not null," +
                                "company_name varchar(160)," +
                                "company_number varchar(8)," +
                                "generated_value varchar(8)" +
                                ")");

        importer.performImport("src/test/resources/test-data.csv");

        List<Object[]> dataset = getExpectedDataset(false);

        List<Object[]> expectedData = new ArrayList<Object[]>();
        for (Object[] row : dataset)
        {
            expectedData.add(new Object[]{
                    row[0].toString().toLowerCase(),
                    row[1].toString(),
                    StringUtils.reverse(row[1].toString())});
        }
        assertRecordCount(connection, expectedData, false);

        connection.close();
    }

    private List<Object[]> sortDatasetByCompanyNameAndUpdateDate(List<Object[]> expectedDataset)
    {
        Collections.sort(expectedDataset, new Comparator<Object>()
        {
            @Override
            public int compare(Object o1, Object o2)
            {
                Object[] a1 = (Object[]) o1;
                Object[] a2 = (Object[]) o2;
                return ((String) a1[0] + a1[4]).compareTo((String) a2[0] + a1[4]);
            }
        });
        return expectedDataset;
    }
}
