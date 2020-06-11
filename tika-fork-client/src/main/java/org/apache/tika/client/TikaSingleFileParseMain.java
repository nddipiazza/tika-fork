package org.apache.tika.client;

import org.apache.tika.metadata.Metadata;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;

public class TikaSingleFileParseMain {

  @Option(name = "-filePath", usage = "File to test with.")
  private String filePath;
  @Option(name = "-contentType", usage = "Content type of the file.")
  private String contentType;
  @Option(name = "-contentInServerPort", usage = "This is the port for the socket server that will be used to send in the file.")
  private int contentInServerPort = 0;
  @Option(name = "-metadataOutServerPort", usage = "This is the port for the socket server that will be used to send out the parsed metadata.")
  private int metadataOutServerPort = 0;
  @Option(name = "-contentOutServerPort", usage = "This is the port for the socket server that will be used to send out the parsed file contents.")
  private int contentOutServerPort = 0;

  public static void main (String [] args) throws Exception {
    TikaSingleFileParseMain tikaForkMain = new TikaSingleFileParseMain();
    CmdLineParser cmdLineParser = new CmdLineParser(tikaForkMain);
    cmdLineParser.parseArgument(args);
    tikaForkMain.run();
  }

  public void run() throws Exception {
    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(filePath)) {
      Metadata metadata = tikaRunner.parse(filePath,
        contentType,
        fis,
        contentOutputStream,
        300000L,
        50000000
      );

      System.out.println("Metadata elements parsed: " + metadata.size());

      System.out.println(new String(contentOutputStream.toByteArray()));
    }
  }
}
