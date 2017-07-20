package com.anjlab.csv2db;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ValueDefinitionTest
{
    @Test
    public void toJson()
    {
        Configuration configuration = new Configuration();

        Map<String, ValueDefinition> insertValues = new HashMap<String, ValueDefinition>();
        configuration.setInsertValues(insertValues);

        insertValues.put("column", new StringLiteral("constant"));

        Assert.assertEquals(
                "{\"sqlEscapeChar\":\"\\u0000\",\"insertValues\":{\"column\":\"constant\"},\"batchSize\":100,\"limit\":0,\"forceUpdate\":false,\"ignoreNullPK\":false,\"ignoreDuplicatePK\":false}",
                configuration.toJson());

        insertValues.put("column", new SqlLiteral("clause"));

        Assert.assertEquals(
                "{\"sqlEscapeChar\":\"\\u0000\",\"insertValues\":{\"column\":{\"sql\":\"clause\"}},\"batchSize\":100,\"limit\":0,\"forceUpdate\":false,\"ignoreNullPK\":false,\"ignoreDuplicatePK\":false}",
                configuration.toJson());

        insertValues.put("column", new FunctionReference("name"));

        Assert.assertEquals(
                "{\"sqlEscapeChar\":\"\\u0000\",\"insertValues\":{\"column\":{\"function\":\"name\"}},\"batchSize\":100,\"limit\":0,\"forceUpdate\":false,\"ignoreNullPK\":false,\"ignoreDuplicatePK\":false}",
                configuration.toJson());
    }
}
