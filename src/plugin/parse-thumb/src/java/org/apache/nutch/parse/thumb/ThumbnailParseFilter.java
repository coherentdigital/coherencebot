/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.parse.thumb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.slugify.Slugify;

/**
 * HtmlParseFilter to generate a thumb using thumb.io and store it in S3. The
 * result is returned in parseMeta("thumbnail");
 */
public class ThumbnailParseFilter implements HtmlParseFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private String s3Bucket;
  private String s3RegionString;
  private String thumbServiceUrl;
  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private String thumbApiKey;
  private static final String[] supportedMimeTypes = { "application/pdf" };
  private AmazonS3 s3 = null;

  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    // Check if its a supported mime type.
    String mimeType = content.getContentType();
    List<String> supportedMimeList = Arrays.asList(supportedMimeTypes);
    if (mimeType == null || !supportedMimeList.contains(mimeType)) {
      LOG.debug("Cannot generate a thumbnail for mime type " + mimeType);
      return parseResult;
    }

    // Check if its a significant quantity of text. We don't need thumbnails for
    // near empty pages.
    int contentLength = content.getContent().length;
    if (contentLength < 4000) {
      LOG.debug(
          "Not generating a thumbail; document too small: " + contentLength);
      return parseResult;
    }

    Pattern protocolPattern = Pattern.compile("(file://|http://|https://)");
    Pattern hostPattern = Pattern.compile("([^\\/\\?]+)[\\/\\?](.+)");

    String urlString = content.getUrl();
    String hostPrefix = "";
    String pathSuffix = "";

    // Does the metadata already have a thumbnail?
    String thumbnailUrl = null;
    String archiveUrl = null;
    Parse parse = parseResult.get(urlString);
    if (parse != null) {
      thumbnailUrl = parse.getData().getMeta("thumbnail");
      if (thumbnailUrl == null) {
        thumbnailUrl = parse.getData().getMeta("twitter:image");
      }
      if (thumbnailUrl == null) {
        thumbnailUrl = parse.getData().getMeta("og:image");
      }
      if (thumbnailUrl != null) {
        LOG.debug(
            "Page " + urlString + "already has thumbnail at " + thumbnailUrl);
        return parseResult;
      }
    }

    try {
      // Check if the key already exists for this URL
      boolean keyExists = false;

      // We make a key for the page in the form host.domain.com/page-slug.png
      String urlStringNoProtocol = protocolPattern.matcher(urlString)
          .replaceAll("");
      String s3Key = null;
      Matcher m = hostPattern.matcher(urlStringNoProtocol);
      Slugify slg = new Slugify();
      if (m.find()) {
        hostPrefix = m.group(1);
        pathSuffix = m.group(2);
        s3Key = hostPrefix + "/" + slg.slugify(pathSuffix) + ".png";
      } else {
        s3Key = urlStringNoProtocol + "/" + slg.slugify(urlStringNoProtocol)
            + ".png";
      }

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
          LOG.debug("Thumbnail for " + os.getKey() + " already exists");
          keyExists = true;
          break;
        }
      }

      // Now call the service to make a thumbnail for this URL
      if (!keyExists) {
        String serviceUrl = thumbServiceUrl.replace("{auth}", thumbApiKey);
        // If its not a PDF URL, remove the pdf option from the service request.
        if (!"application/pdf".equals(mimeType)) {
          serviceUrl = serviceUrl.replace("/pdfSource", "");
          serviceUrl = serviceUrl.replace("/fullpage", "");
        }
        try {
          serviceUrl = serviceUrl
              + URLEncoder.encode(urlString, StandardCharsets.UTF_8.toString());
          URL url = new URL(serviceUrl);
          URLConnection con = null;

          // Save the image in S3
          try {
            // Connect to thum.io
            con = url.openConnection();
            con.setConnectTimeout(10000);
            con.setReadTimeout(60000);
            con.connect();

            // Read the thum.io image into a byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(con.getInputStream(), baos);
            baos.close();

            // Convert the byte array into an input stream to S3
            byte[] image = baos.toByteArray();
            if (image.length > 0) {
              ByteArrayInputStream in = new ByteArrayInputStream(image);
              ObjectMetadata s3Meta = new ObjectMetadata();
              s3Meta.setContentType("image/png");
              s3Meta.setContentLength(image.length);
              s3.putObject(new PutObjectRequest(s3Bucket, s3Key, in, s3Meta)
                  .withCannedAcl(CannedAccessControlList.PublicRead));
              LOG.debug("Saved thumbnail in s3 at " + s3Key);
              thumbnailUrl = s3.getUrl(s3Bucket, s3Key).toString();
              archiveUrl = "s3://" + s3Bucket + "/" + s3Key;
            } else {
              LOG.debug("Thumbnail returned is zero bytes for " + serviceUrl);
            }
          } catch (AmazonServiceException e) {
            LOG.error("Can't save the image in S3", e);
          }
        } catch (Exception e) {
          LOG.error("Unable to obtain thumbnail from " + serviceUrl, e);
        }
      } else {
        thumbnailUrl = s3.getUrl(s3Bucket, s3Key).toString();
        archiveUrl = "s3://" + s3Bucket + "/" + s3Key;
      }
    } catch (Exception e) {
      LOG.error("Unable to generate thumbnail for " + urlString, e);
    }

    // Save the thumbnail URLs in the parseMeta
    if (thumbnailUrl != null) {
      parse.getData().getParseMeta().set("thumbnail", thumbnailUrl);
    }
    if (archiveUrl != null) {
      parse.getData().getParseMeta().set("thumbnail.url_archive", archiveUrl);
    }

    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    s3Bucket = conf.getTrimmed("parsefilter.thumb.s3bucket");
    s3RegionString = conf.getTrimmed("parsefilter.thumb.s3region");
    thumbServiceUrl = conf.getTrimmed("parsefilter.thumb.serviceurl");

    // Check that the config has the credentials.
    String credentials = conf.getTrimmed("parsefilter.thumb.credentials");
    if (credentials == null) {
      String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:THUMBIO_APIKEY in config element parserfilter.thumb.credentials. "
          + " This allows using S3 to store results.";
      LOG.error(message);
      LOG.error(Arrays.toString(new Throwable().getStackTrace()).replace( ',', '\n' ));
    } else {
      String[] credParts = credentials.split(":", 3);
      if (credParts.length != 3) {
        String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:THUMBIO_APIKEY in config element parserfilter.thumb.credentials. "
            + " This allows using S3 to store results.";
        LOG.error(message);
      } else {
        awsAccessKeyId = credParts[0];
        awsSecretAccessKey = credParts[1];
        thumbApiKey = credParts[2];
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
}
