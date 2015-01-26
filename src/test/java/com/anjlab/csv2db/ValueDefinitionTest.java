package com.anjlab.csv2db;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ValueDefinitionTest
{
    @Test
    public void toJson()
    {
        Configuration configuration = new Configuration();
        
        Map<String, ValueDefinition> insertValues = new HashMap<String, ValueDefinition>();
        configuration.setInsertValues(insertValues);
        
        insertValues.put("column", new StringLiteral("constant"));
        
        Assert.assertEquals("{\"insertValues\":{\"column\":\"constant\"},\"batchSize\":100,\"forceUpdate\":false,\"ignoreNullPK\":false}",
                configuration.toJson());
        
        insertValues.put("column", new SqlLiteral("clause"));
        
        Assert.assertEquals(
                "{\"insertValues\":{\"column\":{\"sql\":\"clause\"}},\"batchSize\":100,\"forceUpdate\":false,\"ignoreNullPK\":false}",
                configuration.toJson());
        
        insertValues.put("column", new FunctionReference("name"));
        
        Assert.assertEquals(
                "{\"insertValues\":{\"column\":{\"function\":\"name\"}},\"batchSize\":100,\"forceUpdate\":false,\"ignoreNullPK\":false}",
                configuration.toJson());
    }
}
