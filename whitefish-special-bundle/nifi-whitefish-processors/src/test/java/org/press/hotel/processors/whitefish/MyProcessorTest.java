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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.junit.Before;
import org.junit.Test;
import org.press.hotel.models.TripAdvisor;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MyProcessorTest {

	private TestRunner testRunner;

	@Before
	public void init() {

		testRunner = TestRunners.newTestRunner(MyProcessor.class);
	}

	//@Test
	public void testProcessor() throws URISyntaxException, IOException,
			Exception {
		final TestRunner testRunner = TestRunners
				.newTestRunner(new MyProcessor());

		URL url = Thread.currentThread().getContextClassLoader()
				.getResource("bozeman.html");
		Path resPath = Paths.get(url.toURI());
		final String SAMPLE_STRING = new String(Files.readAllBytes(resPath),
				"UTF8");

		testRunner.setProperty(MyProcessor.HOSTNAME, "74.208.225.106");
		testRunner.setProperty(MyProcessor.CLUSTER_NAME, "xanaduCluster");
		testRunner.setProperty(MyProcessor.PORT, "9300");
		testRunner.setProperty(MyProcessor.INDEX, "rankings");
		testRunner.setProperty(MyProcessor.TYPE, "${type}");
		testRunner.setProperty(MyProcessor.SUBREGION, "${subregion}");

		Map<String, String> attrs = new HashMap<>();
		attrs.put("type", "Pressss");
		attrs.put("subregion", "Billings Junior");

		testRunner.enqueue(SAMPLE_STRING.getBytes("UTF-8"), attrs);

		testRunner.run();

		// System.out.println(testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS).get(0).);
	}

	//@Test
	public void shouldWork() throws Exception {

		URL url = Thread.currentThread().getContextClassLoader()
				.getResource("belgrade.html");
		// File file = new File(url.getPath());
		java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
		String xml = new String(java.nio.file.Files.readAllBytes(resPath),
				"UTF8");
		// System.out.println(xml);
		String pattern = "(?:HotelName['][,]\\selement[:]\\sthis[}][)][;]\"\\sdir[=]ltr[>])([\\w\\s&;\\-,\\.#\\\"\\']*)(?:<\\/a>)(?:.*alt=\")(\\d\\.?\\d?)(?: of 5 stars)(?:.*<div class=\"slim_ranking\">#)(\\d+)(?:\\sof\\s)(\\d+)(?:\\shotels)";

		// Pattern r = Pattern.compile(pattern);
		// Matcher m = r.matcher(xml);
		//
		//
		//
		// // Skip the first find because its a sponsored hotel
		// m.find();
		// while (m.find()) {
		// Document d = Jsoup.parse(m.group(0));
		// System.out.println();
		// }
		
		Document doc = Jsoup.connect("http://www.tripadvisor.com/Hotels-g45402-Whitefish_Montana-Hotels.html").userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36").get();
		
		//Document doc = Jsoup.parse(xml);
		Elements eles = doc.select("div[class$=easyClear]");

		List<TripAdvisor> taList = new ArrayList<>();
		for (int i = 2; i < eles.size(); i++) {
			TripAdvisor ta = new TripAdvisor();
			
			try{
			Element element = eles.get(i);
			Elements hotelName = element.select("div[class=listing_title]");
			String hotelNameStr = hotelName.get(0).childNode(0).childNode(0)
					.toString();
			ta.setName(hotelNameStr);
			System.out.println(hotelNameStr);
			Elements ratings = element.select("span[class^=rate]");
			String ratingsStr = ratings.get(0).childNode(0).attr("alt");
			String[] ratingSplit = ratingsStr.split("\\s");
			ta.setRating(Double.valueOf(ratingSplit[0]));

			System.out.println(ratingsStr);
			Elements reviews = element.select("span[class=more]");
			String reviewStr = reviews.get(0).childNode(0).childNode(0)
					.toString();
			System.out.println(reviewStr);
			String[] reviewSplit = reviewStr.split("\\s");
			ta.setReviews(Integer.valueOf(reviewSplit[0]));
			Elements rankings = element.select("div[class=slim_ranking]");
			String rankingStr = rankings.get(0).childNode(0).toString().trim()
					.replace("#", "");
			String[] rankingSplit = rankingStr.split("\\s");
			ta.setRanking(Integer.valueOf(rankingSplit[0]));
			ta.setHotelsInRegion(Integer.valueOf(rankingSplit[2]));
			ta.setReverseRank(ta.getHotelsInRegion() + 1 - ta.getRanking());
			System.out.println(rankingStr);
			Elements tags = element.select("div[class=clickable_tags]");
			List<Node> nodes = tags.get(0).childNodes();
			List<String> tagList = new ArrayList<>();
			for (Node n : nodes) {
				System.out.println(n.childNode(0));
				tagList.add(n.childNode(0).toString());
			}
			ta.setTags(tagList);
			
			Double weighted =  ta.getRating().doubleValue() + (.5 *(ta.getReviews().doubleValue() / 1000));
			ta.setWeightedRating(weighted);
			
			taList.add(ta);
			
			final ObjectMapper mapper = new ObjectMapper();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			mapper.setDateFormat(df);
			
			System.out.println(mapper.writeValueAsString(ta));
			
			System.out.println();
			} catch (Exception e){
				System.out.println("Failed to parse one of the hotels, dropping it");
			}
		}

		Element e = eles.get(0);
		List<Node> nodes = e.childNodes();
		Node node = nodes.get(0);
		System.out.println(node.childNode(0));

		System.out.println(eles.val());
		// System.out.println(eles);

		// final String SAMPLE_STRING =
		// IOUtils.toString(this.getClass().getResourceAsStream("whitefish.html"),"UTF-8");
	}

}
