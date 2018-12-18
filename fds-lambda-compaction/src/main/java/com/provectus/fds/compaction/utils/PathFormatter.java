package com.provectus.fds.compaction.utils;

public class PathFormatter {
    private final String source;
    private final String msgtype;
    private final Period period;
    private final String filename;

    public PathFormatter(String source, String msgtype, Period period, String filename) {
        this.source = source;
        this.msgtype = msgtype;
        this.period = period;
        this.filename = filename;
    }

    public String getSource() {
        return source;
    }

    public String getMsgtype() {
        return msgtype;
    }

    public Period getPeriod() {
        return period;
    }

    public String getFilename() {
        return filename;
    }

    public static PathFormatter fromS3Path(String path) {
        String[] parts = path.split("/");
        String source = parts[0];
        String msgtype = parts[1];
        Period period = Period.fromJsonPath(path.substring(source.length() + msgtype.length() + 1));
        String filename = parts[parts.length-1];
        return new PathFormatter(source, msgtype, period, filename);
    }

    public String path(String source) {
        return String.format("%s/%s/%s", source, this.msgtype, period.path());
    }

    public String pathWithFile(String source, String filename) {
        return String.format("%s/%s/%s/%s", source, this.msgtype, period.path(), filename);
    }
}
