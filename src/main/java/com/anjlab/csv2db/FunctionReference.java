package com.anjlab.csv2db;

import java.util.Arrays;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class FunctionReference implements ValueDefinition
{
    private String functionName;
    
    public FunctionReference(String functionName)
    {
        this.functionName = functionName;
    }
    
    public Object eval(ScriptEngine scriptEngine, Object... args)
    {
        try
        {
            return ((Invocable) scriptEngine).invokeFunction(functionName, args);
        }
        catch (ScriptException | NoSuchMethodException e)
        {
            StringBuilder functionCall =
                    new StringBuilder()
                            .append(functionName)
                            .append('(');

            if (args != null)
            {
                functionCall.append(Arrays.asList(args));
            }

            functionCall.append(')');

            throw new RuntimeException("Error calling " + functionCall, e);
        }
    }

    @Override
    public Object eval(String targetTableColumnName, Map<String, String> nameValues, ScriptEngine scriptEngine)
    {
        return eval(scriptEngine, targetTableColumnName, nameValues);
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
