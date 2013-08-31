package com.anjlab.csv2db;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ValueDefinitionAdapter
    implements JsonSerializer<ValueDefinition>, JsonDeserializer<ValueDefinition>
{

    @Override
    public ValueDefinition deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException
    {
        if (json instanceof JsonPrimitive)
        {
            return new StringLiteral(json.getAsString());
        }
        else
        {
            JsonObject definition = json.getAsJsonObject();
            
            if (definition.get("function") != null)
            {
                return new FunctionReference(definition.get("function").getAsString());
            }
            
            if (definition.get("sql") != null)
            {
                return new SqlLiteral(definition.get("sql").getAsString());
            }
            
            throw new JsonParseException("Unsupported value definition: " + definition);
        }
    }

    @Override
    public JsonElement serialize(ValueDefinition src, Type typeOfSrc,
            JsonSerializationContext context)
    {
        return src.toJsonElement();
    }

}
