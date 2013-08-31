package com.anjlab.csv2db;

import java.text.MessageFormat;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class FunctionReference implements ValueDefinition
{
    private static final Gson GSON = new Gson();
    
    private String functionName;
    
    public FunctionReference(String functionName)
    {
        this.functionName = functionName;
    }
    
    @Override
    public Object eval(String targetTableColumnName, Map<String, String> nameValues, ScriptEngine scriptEngine)
    {
        StringBuilder functionCall = new StringBuilder();
        functionCall.append(functionName)
                    .append("('")
                    .append(targetTableColumnName).append("',").append(GSON.toJson(nameValues))
                    .append(')');
        try
        {
            return scriptEngine.eval(functionCall.toString());
        }
        catch (ScriptException e)
        {
            throw new RuntimeException("Error calling " + functionCall, e);
        }
    }
    
    @Override
    public JsonElement toJsonElement()
    {
        JsonObject json = new JsonObject();
        json.addProperty("function", functionName);
        return json;
    }
    
    @Override
    public boolean producesSQL()
    {
        return false;
    }
}
