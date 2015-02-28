package com.anjlab.csv2db;

import java.util.Collection;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class FunctionReference implements ValueDefinition
{
    private static final Gson GSON = new Gson();
    
    private String functionName;
    
    public FunctionReference(String functionName)
    {
        this.functionName = functionName;
    }
    
    public Object eval(ScriptEngine scriptEngine, Object... args)
    {
        StringBuilder functionCall = new StringBuilder();
        functionCall.append(functionName)
                    .append('(');

        if (args != null)
        {
            buildArgumentsList(functionCall, args);
        }

        functionCall.append(')');

        try
        {
            return scriptEngine.eval(functionCall.toString());
        }
        catch (ScriptException e)
        {
            throw new RuntimeException("Error calling " + functionCall, e);
        }
    }

    private void buildArgumentsList(StringBuilder functionCall, Object... args)
    {
        boolean firstArg = true;

        for (Object arg : args)
        {
            if (firstArg)
            {
                firstArg = false;
            }
            else
            {
                functionCall.append(',');
            }

            if (arg == null)
            {
                functionCall.append("null");
            }
            else if (arg instanceof String)
            {
                functionCall.append('\'').append(arg).append('\'');
            }
            else if (arg instanceof Number || arg instanceof Boolean || arg.getClass().isPrimitive())
            {
                functionCall.append(arg);
            }
            else if (arg instanceof Map<?, ?> || arg instanceof Collection<?>)
            {
                functionCall.append(GSON.toJson(arg));
            }
            else if (arg instanceof FunctionReference)
            {
                //  Pass as function name literal
                functionCall.append(((FunctionReference) arg).functionName);
            }
            else
            {
                throw new IllegalArgumentException("Unsupported argument type: " + arg);
            }
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
