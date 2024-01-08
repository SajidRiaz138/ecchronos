/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

public final class EccYamlToMarkdownConverter
{

    private static final Logger LOG = LoggerFactory.getLogger(EccYamlToMarkdownConverter.class);
    private static final String SEPARATOR = "---";

    private EccYamlToMarkdownConverter()
    {
    }

    public static void main(final String[] args)
    {
        try
        {
            convertYamlToMarkdown();
        }
        catch (IOException e)
        {
            LOG.error("Error converting YAML to Markdown: {}", e.getMessage(), e);
        }
    }

    private static void convertYamlToMarkdown() throws IOException
    {
        final String relativePathToDocs = "docs/autogenerated";
        final String markdownFileName = "EccYamlFile.md";
        final String yamlFilePath = "/ecc.yml"; // Resource path in the classpath
        final String currentWorkingDir = System.getProperty("user.dir");
        final String markdownFilePath = Paths.get(currentWorkingDir, relativePathToDocs, markdownFileName).toString();

        try (InputStream inputStream = EccYamlToMarkdownConverter.class.getResourceAsStream(yamlFilePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                BufferedWriter writer = new BufferedWriter(new FileWriter(markdownFilePath)))
        {

            if (inputStream == null)
            {
                throw new FileNotFoundException("Resource not found: " + yamlFilePath);
            }

            boolean skipLicenseHeader = true; // Skip license header
            String line = reader.readLine();
            while (line != null)
            {
                if (skipLicenseHeader && line.startsWith("#"))
                {
                    line = reader.readLine();
                    continue; // Skip license header lines
                }
                skipLicenseHeader = false;

                String markdownLine = convertLineToMarkdown(line);
                writer.write(markdownLine);
                writer.newLine();
                line = reader.readLine();
            }
        }
    }

    private static String convertLineToMarkdown(final String line)
    {
        String trimmedLine = line.trim();

        // Handle empty lines and separators
        if (trimmedLine.isEmpty() || SEPARATOR.equals(trimmedLine))
        {
            return "";
        }

        // Convert comment lines to regular text (not bold)
        if (trimmedLine.startsWith("#"))
        {
            return trimmedLine.substring(1).trim();
        }

        // Bold section headers (e.g., "repair:" becomes "**repair:**")
        if (trimmedLine.endsWith(":") && !trimmedLine.contains(" "))
        {
            return "**" + trimmedLine + "**";
        }

        // Format key-value pairs (e.g., "host: localhost" becomes "* host: localhost*")
        if (trimmedLine.contains(": ") && !trimmedLine.startsWith(" "))
        {
            return "* " + trimmedLine;
        }

        return line;
    }
}
