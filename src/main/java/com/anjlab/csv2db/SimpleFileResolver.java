package com.anjlab.csv2db;

import java.io.File;

public class SimpleFileResolver implements FileResolver
{
    private final File currentDir;
    
    public SimpleFileResolver(String currentDir)
    {
        this(new File(currentDir));
    }
    
    public SimpleFileResolver(File currentDir)
    {
        this.currentDir = currentDir;
    }
    
    @Override
    public File getFile(String filename)
    {
        return new File(currentDir, filename);
    }
}
