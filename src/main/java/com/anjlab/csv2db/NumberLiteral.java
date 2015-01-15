package com.anjlab.csv2db;

import java.util.Map;

import javax.script.ScriptEngine;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class NumberLiteral implements ValueDefinition
{
    private final Number value;
    
    public NumberLiteral(Number value)
    {
        this.value = value;
    }
    
    @Override
    public Object eval(String targetTableColumnName, Map<String, String> nameValues, ScriptEngine scriptEngine)
    {
        return value;
    }
    
    @Override
    public JsonElement toJsonElement()
    {
        return new JsonPrimitive(value);
    }
    
    @Override
    public boolean producesSQL()
    {
        return false;
    }
}
