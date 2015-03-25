package com.anjlab.csv2db;

import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.google.gson.JsonElement;

public interface ValueDefinition
{
    JsonElement toJsonElement();

    Object eval(String targetTableColumnName, Map<String, Object> nameValues, ScriptEngine scriptEngine) throws ScriptException;

    boolean producesSQL();
}
