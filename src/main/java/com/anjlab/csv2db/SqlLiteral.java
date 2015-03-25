package com.anjlab.csv2db;

import java.util.Map;

import javax.script.ScriptEngine;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SqlLiteral implements ValueDefinition
{
    private final String value;
    
    public SqlLiteral(String value)
    {
        this.value = value;
    }
    
    @Override
    public JsonElement toJsonElement()
    {
        JsonObject json = new JsonObject();
        json.addProperty("sql", value);
        return json;
    }

    @Override
    public Object eval(String targetTableColumnName,
            Map<String, Object> nameValues, ScriptEngine scriptEngine)
    {
        return value;
    }

    @Override
    public boolean producesSQL()
    {
        return true;
    }
}
