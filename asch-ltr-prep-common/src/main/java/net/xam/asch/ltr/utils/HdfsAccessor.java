package net.xam.asch.ltr.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HdfsAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsAccessor.class);

  private static HdfsAccessor instance;
  private final FileSystem fs;
  private Configuration conf;

  public static HdfsAccessor getInstance() {
    if (instance == null) {
      synchronized (HdfsAccessor.class) {
        if (instance == null) {
          try {
            instance = new HdfsAccessor();
          } catch (IOException e) {
            LOG.error("Create hdfsAccessor exception " + e);
          }
        }
      }
    }
    return instance;
  }

  private HdfsAccessor() throws IOException {
    this.conf = new HdfsConfiguration();
    this.fs = FileSystem.get(this.conf);
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public void write(String path, InputStream inputStream) throws IOException {
    Path filePath = new Path(path);
    FSDataOutputStream outputStream = fs.create(filePath, true);
    IOUtils.copyLarge(inputStream, outputStream);

    outputStream.flush();
    outputStream.close();
    inputStream.close();
    LOG.info("write data to hdfs: " + filePath);
  }

  public void writeLines(String path, List<String> lines) throws IOException {
    Path filePath = new Path(path);
    FSDataOutputStream outputStream = fs.create(filePath, true);
    OutputStreamWriter ow = new OutputStreamWriter(outputStream);
    BufferedWriter bw = new BufferedWriter(ow);
    int i = 0;
    for (String line : lines) {
      bw.write(line);
      bw.newLine();
      i++;
      if (i % 1000 == 0) {
        bw.flush();
      }
    }
    bw.flush();
    bw.close();
    ow.close();
    LOG.info("write data to hdfs: " + filePath);
  }

  public void delete(String path) throws IOException {
    Path filePath = new Path(path);
    fs.delete(filePath, false);
    LOG.info("remove data from hbase: " + filePath);
  }

  public InputStream read(String path) throws IOException {
    return fs.open(new Path(path));
  }

  public List<String> readLines(String path) throws IOException {
    List<String> content = new ArrayList<>();
    Path filePath = new Path(path);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath), StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      while (line != null) {
        content.add(line);
        line = reader.readLine();
      }
    }
    return content;
  }

  public boolean exists(String path) throws IOException {
    return fs.exists(new Path(path));
  }

  public boolean mkdirs(String path) throws IOException {
    return fs.mkdirs(new Path(path));
  }

  public FileStatus getFileStatus(String path) throws IOException {
    return fs.getFileStatus(new Path(path));
  }

  public long getSize(String path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(new Path(path));
    if (fileStatus == null) {
      return 0;
    }
    return fileStatus.getLen();
  }

  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        LOG.error("", e);
      }
    }
  }
}