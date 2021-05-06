package org.apache.nutch.parse.pdfheadings;

import static org.apache.nutch.parse.pdfheadings.PDMetadataExtractor.addMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.AccessPermissions;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.AccessChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * This local implementation of Tika's PDF Parser allows this module to call a
 * local (PdfBox) PDFTextSplitter that provides font information in the parsed
 * result.
 * 
 * Since its intended use is for Heading extraction, it does not function as
 * full featured parser.
 * 
 * @author pciuffetti
 */
public class PDFParser extends org.apache.tika.parser.pdf.PDFParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final long serialVersionUID = 1L;
  private static final MediaType MEDIA_TYPE = MediaType.application("pdf");
  private PDFParserConfig defaultConfig = new PDFParserConfig();

  public PDFParser() {
    super();
  }

  @Override
  public void parse(InputStream stream, ContentHandler handler,
      Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    PDFParserConfig localConfig = context.get(PDFParserConfig.class,
        defaultConfig);
    if (localConfig.getSetKCMS()) {
      System.setProperty("sun.java2d.cmm",
          "sun.java2d.cmm.kcms.KcmsServiceProvider");
    }

    PDDocument pdfDocument = null;

    String password = "";
    try {
      MemoryUsageSetting memoryUsageSetting = MemoryUsageSetting
          .setupMainMemoryOnly();
      pdfDocument = getPDDocument(new CloseShieldInputStream(stream), password,
          memoryUsageSetting, metadata, context);
      metadata.set(PDF.IS_ENCRYPTED,
          Boolean.toString(pdfDocument.isEncrypted()));

      metadata.set(Metadata.CONTENT_TYPE, MEDIA_TYPE.toString());
      extractMetadata(pdfDocument, metadata, context);
      AccessChecker checker = localConfig.getAccessChecker();
      checker.check(metadata);
      if (handler != null) {
        PDF2XHTML.process(pdfDocument, handler, context, metadata, localConfig);
      }
    } catch (InvalidPasswordException e) {
      metadata.set(PDF.IS_ENCRYPTED, "true");
      throw new EncryptedDocumentException(e);
    } finally {
      if (pdfDocument != null) {
        pdfDocument.close();
      }
    }
  }

  private void extractMetadata(PDDocument document, Metadata metadata,
      ParseContext context) throws TikaException {

    // first extract AccessPermissions
    AccessPermission ap = document.getCurrentAccessPermission();
    metadata.set(AccessPermissions.EXTRACT_FOR_ACCESSIBILITY,
        Boolean.toString(ap.canExtractForAccessibility()));
    metadata.set(AccessPermissions.EXTRACT_CONTENT,
        Boolean.toString(ap.canExtractContent()));
    metadata.set(AccessPermissions.ASSEMBLE_DOCUMENT,
        Boolean.toString(ap.canAssembleDocument()));
    metadata.set(AccessPermissions.FILL_IN_FORM,
        Boolean.toString(ap.canFillInForm()));
    metadata.set(AccessPermissions.CAN_MODIFY,
        Boolean.toString(ap.canModify()));
    metadata.set(AccessPermissions.CAN_MODIFY_ANNOTATIONS,
        Boolean.toString(ap.canModifyAnnotations()));
    metadata.set(AccessPermissions.CAN_PRINT, Boolean.toString(ap.canPrint()));
    metadata.set(AccessPermissions.CAN_PRINT_DEGRADED,
        Boolean.toString(ap.canPrintDegraded()));

    if (document.getDocumentCatalog().getLanguage() != null) {
      metadata.set(TikaCoreProperties.LANGUAGE,
          document.getDocumentCatalog().getLanguage());
    }
    if (document.getDocumentCatalog().getAcroForm() != null
        && document.getDocumentCatalog().getAcroForm().getFields() != null
        && document.getDocumentCatalog().getAcroForm().getFields().size() > 0) {
      metadata.set(PDF.HAS_ACROFORM_FIELDS, "true");
    }
    PDMetadataExtractor.extract(document.getDocumentCatalog().getMetadata(),
        metadata, context);

    PDDocumentInformation info = document.getDocumentInformation();
    metadata.set(PagedText.N_PAGES, document.getNumberOfPages());
    addMetadata(metadata, PDF.DOC_INFO_TITLE, info.getTitle());
    addMetadata(metadata, PDF.DOC_INFO_CREATOR, info.getAuthor());
    // if this wasn't already set by xmp, use doc info
    if (metadata.get(TikaCoreProperties.CREATOR) == null) {
      addMetadata(metadata, TikaCoreProperties.CREATOR, info.getAuthor());
    }
    if (metadata.get(TikaCoreProperties.TITLE) == null) {
      addMetadata(metadata, TikaCoreProperties.TITLE, info.getTitle());
    }
    addMetadata(metadata, TikaCoreProperties.CREATOR_TOOL, info.getCreator());
    addMetadata(metadata, PDF.DOC_INFO_CREATOR_TOOL, info.getCreator());
    addMetadata(metadata, TikaCoreProperties.KEYWORDS, info.getKeywords());
    addMetadata(metadata, PDF.DOC_INFO_KEY_WORDS, info.getKeywords());
    addMetadata(metadata, "producer", info.getProducer());
    addMetadata(metadata, PDF.DOC_INFO_PRODUCER, info.getProducer());

    addMetadata(metadata, PDF.DOC_INFO_SUBJECT, info.getSubject());

    addMetadata(metadata, "trapped", info.getTrapped());
    // TODO Remove these in Tika 2.0
    Calendar created = info.getCreationDate();
    addMetadata(metadata, PDF.DOC_INFO_CREATED, created);
    addMetadata(metadata, TikaCoreProperties.CREATED, created);
    Calendar modified = info.getModificationDate();
    // addMetadata(metadata, Metadata.LAST_MODIFIED, modified);
    // Overrideany current Last-Modified
    // With the internal PDF value
    // This ensures copied PDFs still get a correct Date
    SimpleDateFormat dateFormat = new SimpleDateFormat(
        "EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    if (modified != null) {
      metadata.set(Metadata.LAST_MODIFIED,
          dateFormat.format(modified.getTime()));
    } else if (created != null) {
      metadata.set(Metadata.LAST_MODIFIED,
          dateFormat.format(created.getTime()));
    }
    addMetadata(metadata, TikaCoreProperties.MODIFIED, modified);
    addMetadata(metadata, PDF.DOC_INFO_MODIFICATION_DATE, modified);

    // All remaining metadata is custom
    // Copy this over as-is
    List<String> handledMetadata = Arrays.asList("Author", "Creator",
        "CreationDate", "ModDate", "Keywords", "Producer", "Subject", "Title",
        "Trapped");
    for (COSName key : info.getCOSObject().keySet()) {
      String name = key.getName();
      if (!handledMetadata.contains(name)) {
        addMetadata(metadata, name,
            info.getCOSObject().getDictionaryObject(key));
        addMetadata(metadata, PDF.PDF_DOC_INFO_CUSTOM_PREFIX + name,
            info.getCOSObject().getDictionaryObject(key));
      }
    }

    // try to get the various versions
    // Caveats:
    // there is currently a fair amount of redundancy
    // TikaCoreProperties.FORMAT can be multivalued
    // There are also three potential pdf specific version keys: pdf:PDFVersion,
    // pdfa:PDFVersion, pdf:PDFExtensionVersion
    metadata.set(PDF.PDF_VERSION,
        Float.toString(document.getDocument().getVersion()));
    metadata.add(TikaCoreProperties.FORMAT.getName(), MEDIA_TYPE.toString()
        + "; version=" + Float.toString(document.getDocument().getVersion()));

    // TODO: Let's try to move this into PDFBox.
    // Attempt to determine Adobe extension level, if present:
    COSDictionary root = document.getDocumentCatalog().getCOSObject();
    COSDictionary extensions = (COSDictionary) root
        .getDictionaryObject(COSName.getPDFName("Extensions"));
    if (extensions != null) {
      for (COSName extName : extensions.keySet()) {
        // If it's an Adobe one, interpret it to determine the extension level:
        if (extName.equals(COSName.getPDFName("ADBE"))) {
          COSDictionary adobeExt = (COSDictionary) extensions
              .getDictionaryObject(extName);
          if (adobeExt != null) {
            String baseVersion = adobeExt
                .getNameAsString(COSName.getPDFName("BaseVersion"));
            int el = adobeExt.getInt(COSName.getPDFName("ExtensionLevel"));
            // -1 is sentinel value that something went wrong in getInt
            if (el != -1) {
              metadata.set(PDF.PDF_EXTENSION_VERSION,
                  baseVersion + " Adobe Extension Level " + el);
              metadata.add(TikaCoreProperties.FORMAT.getName(),
                  MEDIA_TYPE.toString() + "; version=\"" + baseVersion
                      + " Adobe Extension Level " + el + "\"");
            }
          }
        } else {
          // WARN that there is an Extension, but it's not Adobe's, and so is a
          // 'new' format'.
          metadata.set("pdf:foundNonAdobeExtensionName", extName.getName());
        }
      }
    }
  }
}
