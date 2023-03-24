/*
 * Copyright 2021 Coherent Digital LLC
 * https://coherentdigital.net
 */
package org.apache.nutch.analysis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.slugify.Slugify;

/**
 * The ArchiveParseFilter will Archive fetched content.
 *
 * Initial use is to archive PDFs in the event they become unpublished.
 * It's implemented as a parse filter to allow discretion over what
 * gets archived and what is not archived.
 *
 * @author Peter Ciuffetti
 */
public class ArchiveParseFilter implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private String s3Bucket;
  private String s3Prefix;
  private String s3RegionString;
  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private static final String[] supportedMimeTypes = { "application/pdf" };
  private AmazonS3 s3 = null;

  public ArchiveParseFilter() throws IOException {
  }

  /**
   * Archive the document's content.
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    // Check if its a supported mime type.
    String mimeType = content.getContentType();
    List<String> supportedMimeList = Arrays.asList(supportedMimeTypes);
    if (mimeType == null || !supportedMimeList.contains(mimeType)) {
      LOG.debug("Not configured to archive file with mime type " + mimeType);
      return parseResult;
    }

    String urlString = content.getUrl();
    Parse parse = parseResult.get(urlString);
    String digest = parse.getData().getMeta("digest");
    if (digest == null) {
      LOG.warn("Unable to find precomputed digest for " + urlString);
      // Calculate a new one...
      byte[] signature = SignatureFactory.getSignature(conf)
          .calculate(content, parse);
      digest = StringUtil.toHexString(signature);
      LOG.debug("Computed digest is " + digest);
    }

    // Create an S3 prefix based on the file's hostname in the URL
    Pattern protocolPattern = Pattern.compile("(file://|http://|https://)");
    Pattern hostPattern = Pattern.compile("([^\\/\\?]+)[\\/\\?](.+)");
    String hostPrefix = "";
    String pathSuffix = "";
    String urlStringNoProtocol = protocolPattern.matcher(urlString)
        .replaceAll("");
    String s3Key = null;
    Matcher m = hostPattern.matcher(urlStringNoProtocol);
    Slugify slg = new Slugify();
    if (m.find()) {
      hostPrefix = m.group(1);
      pathSuffix = m.group(2);
      if (digest != null) {
        s3Key = s3Prefix + hostPrefix + "/" + digest + ".pdf";
      } else {
        s3Key = s3Prefix + hostPrefix + "/" + slg.slugify(pathSuffix) + ".pdf";
      }
    } else {
      if (digest != null) {
        s3Key = s3Prefix + "no-host/" + digest + ".pdf";
      } else {
        s3Key = s3Prefix + "no-host/" + slg.slugify(urlStringNoProtocol) + ".pdf";
      }
    }

    // Save the PDF in S3
    try {
      // Check if the key already exists for this URL
      boolean keyExists = false;
      // Search s3 for the key...
      if (s3 == null) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId,
            awsSecretAccessKey);
        s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withRegion(Regions.fromName(s3RegionString)).build();
      }

      ListObjectsV2Result result = s3.listObjectsV2(s3Bucket, s3Key);
      List<S3ObjectSummary> objects = result.getObjectSummaries();
      for (S3ObjectSummary os : objects) {
        if (os.getKey().equals(s3Key)) {
          LOG.info("Archive for " + os.getKey() + " already exists");
          keyExists = true;
          break;
        }
      }

      String archiveUrl = "s3://" + s3Bucket + "/" + s3Key;
      if (!keyExists) {
        byte[] rawContent = content.getContent();
        if (rawContent.length > 0) {
          ByteArrayInputStream in = new ByteArrayInputStream(rawContent);
          ObjectMetadata s3Meta = new ObjectMetadata();
          s3Meta.setContentType(mimeType);
          s3Meta.setContentLength(rawContent.length);
          s3.putObject(new PutObjectRequest(s3Bucket, s3Key, in, s3Meta));
          LOG.debug("Saved file in s3 at " + s3Key);
          parse.getData().getParseMeta().set("file.url_archive", archiveUrl);
        } else {
          LOG.debug("Zero bytes for " + urlString);
        }
      } else {
        parse.getData().getParseMeta().set("file.url_archive", archiveUrl);
      }
    } catch (AmazonServiceException e) {
      LOG.error("Can't save the file in S3", e);
    }

    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    s3Bucket = conf.getTrimmed("parsefilter.archive.s3bucket");
    s3Prefix = conf.getTrimmed(
        "parsefilter.archive.s3prefix",
        "file/coherencebot-archive/"
    );
    s3RegionString = conf.getTrimmed("parsefilter.archive.s3region");

    // Check that the config has the credentials.
    String credentials = conf.getTrimmed("parsefilter.archive.credentials");
    if (credentials == null) {
      String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY in config element parserfilter.archive.credentials. "
          + " This allows using S3 to store results.";
      LOG.error(message);
      LOG.error(Arrays.toString(new Throwable().getStackTrace()).replace( ',', '\n' ));
    } else {
      String[] credParts = credentials.split(":");
      if (credParts.length != 2) {
        String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY in config element parserfilter.archive.credentials. "
            + " This allows using S3 to store results.";
        LOG.error(message);
      } else {
        awsAccessKeyId = credParts[0];
        awsSecretAccessKey = credParts[1];
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
}
