package com.anjlab.csv2db;

import java.util.Map;

import javax.script.ScriptEngine;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class BooleanLiteral implements ValueDefinition
{
    private final boolean value;
    
    public BooleanLiteral(boolean value)
    {
        this.value = value;
    }
    
    @Override
    public Object eval(String targetTableColumnName, Map<String, Object> nameValues, ScriptEngine scriptEngine)
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
