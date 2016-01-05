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
package org.press.hotel.processors.whitefish;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.press.hotel.models.TripAdvisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Tags({ "GUILE ONLY" })
@CapabilityDescription("Provides a Quad City DJ's SLAM JAM level of funk")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MyProcessor extends AbstractProcessor {

	// private static final String HOSTNAME = "74.208.225.106"; // for local
	// private static final int PORT = 9300;
	// private static final String XANADU_CLUSTER = "xanaduCluster";

	public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
			.name("Hostname").description("ES Hostname").required(true)
			.defaultValue("74.208.225.106")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
			.name("Port").description("ES Port").required(true)
			.defaultValue("9300")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
			.name("Cluster").description("ES Cluster Name").required(true)
			.defaultValue("xanaduCluster")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
			.name("Index").description("ES Index Name").required(true)
			.defaultValue("rankings")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
			.name("Type").description("ES Type Name").required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor SUBREGION = new PropertyDescriptor.Builder()
			.name("SubRegion")
			.description(
					"Sub Region for a larger region ( Kalispell or Whitefish ) ")
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success").description("Successfully did the thing").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure").description("Failed the thing").build();

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private static TransportClient CLIENT = null;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
		props.add(HOSTNAME);
		props.add(PORT);
		props.add(CLUSTER_NAME);
		props.add(INDEX);
		props.add(TYPE);
		props.add(SUBREGION);
		this.properties = Collections.unmodifiableList(props);

		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_SUCCESS);
		rels.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(rels);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context)
			throws UnknownHostException {
		final String CLUSTER_NAME_PROPERTY = "cluster.name";
		// System.out.println(context.getProperty(CLUSTER_NAME));
		// Settings settings = Settings
		// .settingsBuilder()
		// .put(CLUSTER_NAME_PROPERTY,
		// context.getProperty(CLUSTER_NAME).toString()).build();
		// CLIENT = TransportClient
		// .builder()
		// .settings(settings)
		// .build()
		// .addTransportAddress(
		// new InetSocketTransportAddress(InetAddress
		// .getByName(context.getProperty(HOSTNAME)
		// .getValue()), context.getProperty(PORT)
		// .asInteger()));

		Settings settings = ImmutableSettings
				.settingsBuilder()
				.put(CLUSTER_NAME_PROPERTY,
						context.getProperty(CLUSTER_NAME).toString()).build();
		CLIENT = new TransportClient(settings);
		CLIENT.addTransportAddress(new InetSocketTransportAddress(context
				.getProperty(HOSTNAME).getValue(), context.getProperty(PORT)
				.asInteger()));
	}

	@OnStopped
	public void onStopped() {
		CLIENT.close();
	}

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final ProcessorLog logger = getLogger();
		final String contentString;
		final byte[] buffer = new byte[(int) flowFile.getSize()];
		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream in) throws IOException {
				StreamUtils.fillBuffer(in, buffer);
			}
		});

		contentString = new String(buffer);
		// This whole block checks to see if an index exists, if it doesn't
		// create it. It also creates a mapping file so that the fucking name
		// field isn't analyzed so its not fucking split up when we visualize in
		// kibana. You're welcome everyone. -Press
		try {

			boolean hasIndex = CLIENT
					.admin()
					.indices()
					.exists(new IndicesExistsRequest(context.getProperty(INDEX)
							.getValue())).actionGet().isExists();

			if (!hasIndex) {
				CLIENT.admin()
						.indices()
						.create(new CreateIndexRequest(context.getProperty(
								INDEX).getValue())).actionGet();
			}

			CLIENT.admin()
					.indices()
					.preparePutMapping(context.getProperty(INDEX).getValue())
					.setType(
							context.getProperty(TYPE)
									.evaluateAttributeExpressions(flowFile)
									.getValue())
					.setSource(
							XContentFactory
									.jsonBuilder()
									.prettyPrint()
									.startObject()
									.startObject(
											context.getProperty(TYPE)
													.evaluateAttributeExpressions(
															flowFile)
													.getValue())
									.startObject("properties")
									.startObject("name")
									.field("type", "string")
									.field("index", "not_analyzed").endObject()
									.endObject().endObject().endObject())
					.execute().actionGet();

		} catch (ElasticsearchException e1) {

			e1.printStackTrace();
		} catch (IOException e1) {

			e1.printStackTrace();
		}

		// Document doc = null;
		// try {
		// doc =
		// Jsoup.connect("http://www.tripadvisor.com/Hotels-g45086-Billings_Montana-Hotels.html").userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36").get();
		// } catch (IOException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }

		Document doc = Jsoup.parse(contentString);
		Elements eles = doc.select("div[class$=easyClear]");

		List<TripAdvisor> taList = new ArrayList<>();
		for (int i = 1; i < eles.size(); i++) {
			TripAdvisor ta = new TripAdvisor();
			try {
				Element element = eles.get(i);
				
				//Check to see if the hotel we have here is sponsored or not.
				if (element.select("div[id=sponsoredHeadingTag").isEmpty()) {
					Elements hotelName = element
							.select("div[class=listing_title]");
					String hotelNameStr = hotelName.get(0).childNode(0)
							.childNode(0).toString();
					ta.setName(hotelNameStr);
					// System.out.println(hotelNameStr);
					Elements ratings = element.select("span[class^=rate]");
					String ratingsStr = ratings.get(0).childNode(0).attr("alt");
					String[] ratingSplit = ratingsStr.split("\\s");
					ta.setRating(Double.valueOf(ratingSplit[0]));

					// System.out.println(ratingsStr);
					Elements reviews = element.select("span[class=more]");
					String reviewStr = reviews.get(0).childNode(0).childNode(0)
							.toString();
					// System.out.println(reviewStr);
					String[] reviewSplit = reviewStr.split("\\s");
					ta.setReviews(Integer.valueOf(reviewSplit[0].replace(",",
							"")));
					Elements rankings = element
							.select("div[class=slim_ranking]");
					String rankingStr = rankings.get(0).childNode(0).toString()
							.trim().replace("#", "");
					String[] rankingSplit = rankingStr.split("\\s");
					ta.setRanking(Integer.valueOf(rankingSplit[0]));
					ta.setHotelsInRegion(Integer.valueOf(rankingSplit[2]));
					ta.setReverseRank(ta.getHotelsInRegion() + 1
							- ta.getRanking());
					// System.out.println(rankingStr);
					Elements tags = element.select("div[class=clickable_tags]");
					List<Node> nodes = tags.get(0).childNodes();
					List<String> tagList = new ArrayList<>();
					for (Node n : nodes) {
						// System.out.println(n.childNode(0));
						tagList.add(n.childNode(0).toString());
					}
					ta.setTags(tagList);

					Double weighted = ta.getRating().doubleValue()
							+ (.5 * (ta.getReviews().doubleValue() / 1000));
					ta.setWeightedRating(weighted);
					ta.setRegion(context.getProperty(TYPE)
							.evaluateAttributeExpressions(flowFile).getValue());

					if (context.getProperty(SUBREGION).isSet()) {
						ta.setSubRegion(context.getProperty(SUBREGION)
								.evaluateAttributeExpressions(flowFile)
								.getValue());

					} else {
						ta.setSubRegion(ta.getRegion());
					}

					taList.add(ta);
				}
			} catch (Exception e) {
				System.out
						.println("Failed to parse one of the hotels, dropping it");
				e.printStackTrace();
			}

		}

		final ObjectMapper mapper = new ObjectMapper();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
		mapper.setDateFormat(df);

		BulkRequestBuilder bulkRequest = CLIENT.prepareBulk();

		for (TripAdvisor t : taList) {
			try {
				//System.out.println(mapper.writeValueAsString(t));
				bulkRequest.add(CLIENT.prepareIndex(
						context.getProperty(INDEX).toString(),
						context.getProperty(TYPE)
								.evaluateAttributeExpressions(flowFile)
								.getValue()).setSource(
						mapper.writeValueAsString(t)));
			} catch (JsonProcessingException e) {
				logger.error("Couldn't convert to json" + e);
			}
		}

		if (bulkRequest.numberOfActions() > 0) {
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				logger.error("We got failures boys "
						+ bulkResponse.buildFailureMessage());
				System.out.println(bulkResponse.buildFailureMessage());
				session.transfer(flowFile, REL_FAILURE);
			}
			session.transfer(flowFile, REL_SUCCESS);
		} else {
			session.transfer(flowFile, REL_FAILURE);
			logger.error("No records passed parsing for file : "
					+ flowFile.getAttribute("Filename"));
		}

		// FlowFile conFlowfile = session.write(flowFile, new StreamCallback() {
		// @Override
		// public void process(InputStream in, OutputStream out)
		// throws IOException {
		// try (OutputStream outputStream = new BufferedOutputStream(out)) {
		// outputStream.write(mapper.writeValueAsBytes(taList));
		// // System.out.println(mapper.writeValueAsString(taList));
		// }
		// }
		// });

		// OLD CODE
		// (?:HotelName['][,]\selement[:]\sthis[}][)][;]"\sdir[=]ltr[>])([\w\s&;\-,\.#\"\']*)(?:<\/a>)(?:.*alt=")(\d\.?\d?)(?:of
		// 5 stars)(?:.*<div
		// class="slim_ranking">#)(\d+)(?:\sof\s)(\d+)(?:\shotels)
		// String pattern =
		// "(?:HotelName['][,]\\selement[:]\\sthis[}][)][;]\"\\sdir[=]ltr[>])([\\w\\s&;\\-,\\.#\\\"\\']*)(?:<\\/a>)(?:.*alt=\")(\\d\\.?\\d?)(?: of 5 stars)(?:.*<div class=\"slim_ranking\">#)(\\d+)(?:\\sof\\s)(\\d+)(?:\\shotels)";
		//
		// Pattern r = Pattern.compile(pattern);
		// Matcher m = r.matcher(contentString);
		//
		// final List<TripAdvisor> taList = new ArrayList<>();
		//
		// // Skip the first find because its a sponsored hotel
		// m.find();
		// while (m.find()) {
		//
		// TripAdvisor ta = new TripAdvisor();
		//
		// // Skipping 0 because capture group 0 is bullshit
		// for (int i = 1; i <= m.groupCount(); i++) {
		// if (i == 1) {
		// ta.setName(m.group(i));
		// } else if (i == 2) {
		// ta.setRating(Double.valueOf(m.group(i)));
		// } else if (i == 3) {
		// ta.setRanking(Integer.valueOf(m.group(i)));
		// } else {
		// ta.setHotelsInRegion(Integer.valueOf(m.group(i)));
		// ta.setReverseRank(ta.getHotelsInRegion() + 1
		// - ta.getRanking());
		// }
		//
		// // System.out.println("Group" + i + " = " + m.group(i));
		//
		// }
		// ta.setRegion(context.getProperty(TYPE)
		// .evaluateAttributeExpressions(flowFile).getValue());
		// taList.add(ta);
		// }

	}
}
