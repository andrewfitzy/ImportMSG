package com.about80minutes.palantir.cedt.msg;

import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;

import com.palantir.api.dataintegration.IDataSource;
import com.palantir.api.dataintegration.transform.AbstractGraphTransformer;
import com.palantir.api.dataintegration.transform.TransformerContext;
import com.palantir.api.dataintegration.util.ContentProcessor;
import com.palantir.api.dataintegration.util.ContentProvider;
import com.palantir.api.dataintegration.util.ImportAPIUtils;
import com.palantir.api.kite.bindings.pxml.Graph;
import com.palantir.api.kite.bindings.pxml.Media;
import com.palantir.api.kite.bindings.pxml.PTObject;
import com.palantir.api.kite.bindings.pxml.Property;
import com.palantir.services.ptobject.LinkType;
import com.palantir.services.ptobject.MediaType;
import com.palantir.services.ptobject.Role;


/**
 * Class for transforming a MSG file into a Palantir object. This class extends
 * AbstractGraphTransformer which requires that a getPxmlGraphBinding is defined.
 */
public class MSGTransformer extends AbstractGraphTransformer<ContentProvider> {
	/**
	 * Constant used to identify the name of this transformer
	 */
	public static final String NAME = "MSGTransformer";

	/**
	 * Constant used to identify the data source which will be used in the kite transformation
	 */
	public static final String DS_HEADER = "MSG_FILE_";

	private static final String OBJECT_SET = "OBJECT_SET";


	/**
	 * Implementation of the getPxmlGraphBinding method as required by
	 * AbstractGraphTransformer. This method is used to process the raw input
	 * and create a Graph to be turned into pxml.
	 *
	 * @param context the {@link com.palantir.api.dataintegration.transform.TransformerContext} for this
	 * transformation
	 * @param input a {@link com.palantir.api.dataintegration.util.ContentProvider} containing the source of
	 * this input
	 *
	 * @return a {@link com.palantir.api.kite.bindings.pxml.Graph} of {@link com.palantir.api.kite.bindings.pxml.PTObject}s
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Graph getPxmlGraphBinding(final TransformerContext context, ContentProvider input) throws Exception {
		Graph graph = new Graph();

		// Create the DataSource instance for our Graph Kite binding.
		IDataSource dSource = context.getDataSource();
		ImportAPIUtils.addDataSource(dSource, graph, null, DS_HEADER);

		// Process the contents of the file and obtain a list of
		// PTObjects and Links which represent Palantir objects and events
		Map<String, Map> graphMap = input.processContent(new MSGProcessor(context, input, graph));

		// Set the object set for the graph to be the objects returned in the graphMap
		graph.setObjectSet(graphMap.get(OBJECT_SET));

		// We're using the default everyone ACL, so there's nothing left to do but
		// return the Graph and let the framework take care of the rest.
		return graph;
	}

	/**
	 * Private inner class implementation of ContentProcessor, this handles the
	 * processing of the raw import content and creates a number of java objects
	 * which represent pxml elements, these get transformed and imported into
	 * Palantir later on by the framework.
	 */
	@SuppressWarnings("rawtypes")
	private class MSGProcessor implements ContentProcessor<Map<String, Map>> {
		private Pattern emailPattern = Pattern.compile("[\\w.%+-]+@[\\w.%-]+\\.[\\w]{2,}");
		private TransformerContext context = null;
		private ContentProvider input = null;
		private Graph graph = null;

		/**
		 * Constructor, simply initialises attributes
		 *
		 * @param context a {@link com.palantir.api.dataintegration.transform.TransformerContext} to use
		 * for this transformation
		 * @param input a {@link com.palantir.api.dataintegration.util.ContentProvider}
		 * for this transformation
		 * @param graph a {@link com.palantir.api.kite.bindings.pxml.Graph} for
		 * this transformation
		 */
		public MSGProcessor(TransformerContext context, ContentProvider input, Graph graph) {
			this.context = context;
			this.input = input;
			this.graph = graph;
		}

		/**
		 * Implementation of the process method, this transforms the source
		 * document into a map of PTObjects which can then be imported into
		 * Palantir
		 *
		 * @param paramInputStream a {@link java.io.InputStream} which is used
		 * as a handle on the documents contents
		 *
		 * @return a {@link java.util.Map<String, Map>} of Maps which are to be
		 * imported, there are essentially two entries in the map, a Map of
		 * links and a Map of objects
		 *
		 * @throws an {@link java.lang.Exception} if there is an error with the
		 * import
		 */
		public Map<String, Map> process(InputStream inputStream) throws Exception {
			//create the main graphSet, this is to be returned by this method
			Map<String, Map> graphSet = new LinkedHashMap<String, Map>();

			//Create the datasourceId to use for the import, this ties the
			//objects to the MSG file being processed
			String dsId = DS_HEADER + context.getDataSource().getId();

			String docName = this.context.getImportArguments().get("SHORT_PATHNAME");
			docName = (docName == null || docName.length()==0) ? input.getShortName() : docName;
			String fullPath = this.context.getImportArguments().get("FULL PATHNAME");
			fullPath = (fullPath == null || fullPath.length()==0) ? input.getFullName() : fullPath;

			//load email message
			MAPIMessage msg = new MAPIMessage(inputStream);

			//Create two maps, one to store the objects and one to store the links between the objects
			Map<String, PTObject> objectSet = new LinkedHashMap<String, PTObject>();

			//Create an object representing the email with title set
			PTObject email = ImportAPIUtils.buildStubObject(dsId, "com.palantir.object.document", "EMAIL_EVENT_");
			email.setTitle(msg.getSubject());
			ImportAPIUtils.addDataSource(graph, email.getId(),dsId,msg.getSubject(), "email");

			Map<String, Property> emailPropertySet = this.extractProperties(dsId, msg, email);

			//set the property set
			email.setPropertySet(emailPropertySet);

			int dsCount=0;
			Media rawMedia = this.buildMedia(this.normalizeLineBreaks(msg.getTextBody()).getBytes("UTF-8"), docName+".txt", "Embedded Text", LinkType.RAW_URI, MediaType.TXT, dsId, "MEDIA_"+(dsCount++));
			email.getMediaSet().put(rawMedia.getId(), rawMedia);

			//set the document as an attachment of itself
			byte[] contents = ContentProvider.Utils.getBytesFromProvider(this.input);
			Media media = this.buildMedia(contents, docName, "Source Media", LinkType.SRC_URI, MediaType.UNSPECIFIED, dsId, "MEDIA_"+(dsCount++));
			email.getMediaSet().put(media.getId(), media);

			AttachmentChunks[] attachments = msg.getAttachmentFiles();
			if(attachments.length > 0) {
				for(AttachmentChunks attachment : attachments) {
					String fileName = attachment.attachFileName.toString();
					if(attachment.attachLongFileName != null) {
					    fileName = attachment.attachLongFileName.toString();
					}
					MediaType type = MediaType.getByFilename(fileName);
					if(type != null) {
						Media tmpMedia = this.buildMedia(attachment.attachData.getValue(), fileName, "Attachment", LinkType.SIMPLE_URI, type, dsId, "MEDIA_"+(dsCount++));
						email.getMediaSet().put(tmpMedia.getId(), tmpMedia);
					} else {
						//ignore, handle later
					}
				}
			}

			//add the object to the objectSet which gets returned in the graphSet from this method
			objectSet.put(email.getId(), email);

			//add the objectSet and linkSet to the graphSet and return
			graphSet.put(OBJECT_SET, objectSet);
			return graphSet;
		}

		/**
		 * Utility method to extract properties to add to the primary object
		 *
		 * @param dsId a {@link java.lang.String} containing the Id of the datasource
		 * @param msg a {@link org.apache.poi.hsmf.MAPIMessage} representing the email message
		 * @param email a {@link com.palantir.api.kite.bindings.pxml.PTObject} representing the pxml object
		 *
		 * @return a {@link java.util.Map} of {@link java.lang.String} properties to
		 * {@link com.palantir.api.kite.bindings.pxml.Property} values
		 *
		 * @throws ChunkNotFoundException
		 * @throws ParseException
		 * @throws DatatypeConfigurationException
		 */
		private Map<String, Property> extractProperties(String dsId, MAPIMessage msg, PTObject email) throws ChunkNotFoundException, ParseException, DatatypeConfigurationException {
			//Initialise property map
			Map<String, Property> emailPropertySet = new LinkedHashMap<String, Property>();

			String[] headers = null;
			try {
				headers = msg.getHeaders();
			} catch (ChunkNotFoundException e) {
				//ignore exception, not supported in POI
			} finally {
				if(headers == null) {
					headers = new String[0];
				}
			}

			//create a property for the date  of the event.
			Calendar dateSent = null;
			try {
				dateSent = msg.getMessageDate();
			} catch (ChunkNotFoundException e) {
				//ignore exception, not supported in POI
			} finally {
				if(dateSent == null && headers.length > 0) {
					//Process the headers into date received
					String date = this.getValueFromHeader(headers, "Date");
					dateSent = this.toCalendar(date);
				}
			}
			if(dateSent != null) {
				DatatypeFactory typeFactory = DatatypeFactory.newInstance();
				XMLGregorianCalendar xmlCal = typeFactory.newXMLGregorianCalendar(
						dateSent.get(Calendar.YEAR), dateSent.get(Calendar.MONTH), dateSent.get(Calendar.DAY_OF_MONTH),
						dateSent.get(Calendar.HOUR_OF_DAY), dateSent.get(Calendar.MINUTE), dateSent.get(Calendar.SECOND),
						dateSent.get(Calendar.MILLISECOND),dateSent.get(Calendar.ZONE_OFFSET));
				Property dateProp = ImportAPIUtils.buildTimeIntervalProperty(email.getId() + "_DATE", xmlCal, xmlCal, dsId);
				emailPropertySet.put(dateProp.getId(), dateProp);
			}

			int propCount=0;
			try {
				String displayFrom = this.getValueFromHeader(msg.getHeaders(), "From");
				List<String> addressList = this.extractEmailAddresses(displayFrom);
				for(String emailaddress : addressList) {
					Property fromEmail = ImportAPIUtils.buildProperty(email.getId() + "_EMAIL"+(propCount++), "com.palantir.property.Email", "com.palantir.link.Simple", Role.FROM.getUri(),emailaddress, dsId);
					emailPropertySet.put(fromEmail.getId(), fromEmail);
				}
			} catch (ChunkNotFoundException e) {
				// ignore
			}
			try {
				String displayTo = this.getValueFromHeader(msg.getHeaders(), "To");
				List<String> addressList = this.extractEmailAddresses(displayTo);
				for(String emailaddress : addressList) {
					Property fromEmail = ImportAPIUtils.buildProperty(email.getId() + "_EMAIL"+(propCount++), "com.palantir.property.Email", "com.palantir.link.Simple", Role.TO.getUri(),emailaddress, dsId);
					emailPropertySet.put(fromEmail.getId(), fromEmail);
				}
			} catch (ChunkNotFoundException e) {
				// ignore
			}
			return emailPropertySet;
		}

		/**
		 * Method to build a media object, this creates media from a given byte array using the details provided
		 *
		 * @param contents a <code>byte[]</code> containing the text to place in the media attachment
		 * @param title a {@link java.lang.String} containing the title of the media
		 * @param description a {@link java.lang.String} containing the description of he media
		 * @param linkType a {@link java.lang.String} containing the URI of the media link type
		 * @param mediaType a {@link com.palantir.services.ptobject.MediaType} representing the type of the media
		 * @param dsId a {@link java.lang.String} containing the Id of the datasource
		 * @param mediaId a {@link java.lang.String} containing the ID of the media item
		 *
		 * @return
		 */
		private Media buildMedia(byte[] contents, String title, String description, String linkType, MediaType mediaType, String dsId, String mediaId) {
		    return this.buildMedia(contents, title, description, linkType, mediaType.getName(), mediaType.getContentType(), dsId, mediaId);
		}

		/**
		 * Method to build a media object, this creates media from a given byte array using the details provided
		 *
		 * @param contents a <code>byte[]</code> containing the text to place in the media attachment
		 * @param title a {@link java.lang.String} containing the title of the media
		 * @param description a {@link java.lang.String} containing the description of he media
		 * @param linkType a {@link java.lang.String} containing the URI of the media link type
		 * @param mediaType a {@link java.lang.String} representing the type of the media
		 * @param contentType a {@link java.lang.String} representing the type of the content
		 * @param dsId a {@link java.lang.String} containing the Id of the datasource
		 * @param mediaId a {@link java.lang.String} containing the ID of the media item
		 *
		 * @return
		 */
		private Media buildMedia(byte[] contents, String title, String description, String linkType, String mediaType, String contentType, String dsId, String mediaId) {
		     Media m = new Media();
		     m.setMediaType(mediaType);
		     m.setMimeType(contentType);
		     m.setMediaData(contents);
		     m.setDataSourceRecordSet(ImportAPIUtils.buildDsrSetFromDsId(dsId));
		     String localLinkType = (null != linkType) ? linkType : LinkType.SIMPLE_URI;
		     m.setLinkType(localLinkType);
		     m.setMediaTitle(title);
		     m.setMediaShortDescription(StringUtils.abbreviate(description, 80));
		     m.setMediaDescription(description);
		     m.setId(mediaId);
		     return m;
		}

		/**
		 * Utility method for creating a Calendar from a date string
		 *
		 * @param displayDate a {@link java.lang.String} containing the display date
		 *
		 * @return a {@link java.util.Calendar} object
		 *
		 * @throws ParseException if the date cannot be processed
		 */
		private Calendar toCalendar(String displayDate) throws ParseException {
			SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
			Date date = format.parse(displayDate);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			return calendar;
		}

		/**
		 * Utility method for extracting a value from the email headers
		 *
		 * @param headers an array of {@link java.lang.String}s containing the headers
		 * @param field a {@link java.lang.String} containing the header field name
		 *
		 * @return a {@link java.lang.String} containing the header value
		 *
		 * @throws ChunkNotFoundException
		 */
		private String getValueFromHeader(String[] headers, String field) throws ChunkNotFoundException {
			StringBuilder value = new StringBuilder();
			String tmpKey = "";
			for(String header : headers) {
				int colonIndex = header.indexOf(":");
				if(!header.startsWith("\t") && colonIndex>=1) {
					tmpKey = header.substring(0,colonIndex);
					if(tmpKey.equalsIgnoreCase(field)) {
						value.append(header.substring(colonIndex+1).trim());
					} else {
						tmpKey = "";
					}
				} else if(tmpKey.equalsIgnoreCase(field) && header.startsWith("\t")) {
					value.append(header.substring("\t".length()).trim());
				} else {
					//ignore
				}
			}
			return value.toString();
		}

		/**
		 * Process a String to extract the matches to an email address regex
		 *
		 * @param inputString the {@link java.lang.String} to process
		 *
		 * @return a {@link java.util.List} of {@link java.lang.String} results
		 */
		private List<String> extractEmailAddresses(String inputString) {
			List<String> emailList = new ArrayList<String>();
			Matcher matcher = emailPattern.matcher(inputString);
			while (matcher.find()) {
				String result = matcher.group();
				emailList.add(result);
			}
			return emailList;
		}

		/**
		 * Utility method for removing excess line breaks from source text.
		 *
		 * @param text an input {@link java.lang.String}
		 *
		 * @return a formatted {@link java.lang.String}
		 */
		private String normalizeLineBreaks(String text) {
			String cleanText = text.replaceAll("\\r\\n", "\n").replaceAll("\\r", "\n").replaceAll("\\n\\n+", "\n\n");
			return cleanText;
		}
	}
}
