package com.provectus.fds.ml.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper utility class for current module
 */
public class IntegrationModuleHelper {
    /**
     * Reads given resource file as a string.
     *
     * @param fileName the path to the resource file
     * @return the file's contents or null if the file could not be opened
     */
    public String getResourceFileAsString(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        return readInputStreamAsString(is);
    }

    public String getFileAsString(Path fileName) throws IOException {
        InputStream is = Files.newInputStream(fileName);
        return readInputStreamAsString(is);
    }

    public String getFileAsString(String fileName) throws IOException {
        InputStream is = Files.newInputStream(Paths.get(fileName));
        return readInputStreamAsString(is);
    }

    private String readInputStreamAsString(InputStream is) {
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        return null;
    }

    /**
     * Returns Greatest Common Divisor
     */
    public int gcd(int a, int b) {
        if (b == 0)
            return a;
        return gcd(b,a % b);
    }

    /**
     * Returns configuration from current environment or if this variable
     * dosn't exists then default value from parameter defVal
     */
    public String getConfig(String key, String defVal) {
        return System.getenv().getOrDefault(key, defVal);
    }

    public String expandEnvVars(String text) {
        Map<String, String> envMap = System.getenv();
        for (Map.Entry<String, String> entry : envMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().replace("\\", "\\\\");
            text = text.replaceAll("\\$\\{" + key + "\\}", value);
        }
        return text;
    }

    private static final String OS = System.getProperty("os.name").toLowerCase();

    public Path getHomePath() {
        if (OS.contains("win")) {
            return Paths.get(expandEnvVars("${HOMEDRIVE}${HOMEPATH}"));
        } else {
            return Paths.get(expandEnvVars("${HOME}"));
        }
    }
}
