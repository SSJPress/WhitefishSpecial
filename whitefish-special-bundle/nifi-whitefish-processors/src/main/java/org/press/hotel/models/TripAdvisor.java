package org.press.hotel.models;

import java.util.Date;
import java.util.List;

public class TripAdvisor {
	private String name;
	private Integer ranking;
	private Date date = new Date(System.currentTimeMillis()-(0*86400000));
	private String region;
	private String subRegion;
	private Double rating;
	private Integer hotelsInRegion;
	private Integer reverseRank;
	private List<String> tags;
	private Double weightedRating;
	private Integer reviews;
	
	public Double getRating() {
		return rating;
	}
	public void setRating(Double rating) {
		this.rating = rating;
	}
	public Integer getHotelsInRegion() {
		return hotelsInRegion;
	}
	public void setHotelsInRegion(Integer hotelsInRegion) {
		this.hotelsInRegion = hotelsInRegion;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getRanking() {
		return ranking;
	}
	public void setRanking(Integer ranking) {
		this.ranking = ranking;
	}
	public Date getDate() {
		return date;
	}
	public Integer getReverseRank() {
		return reverseRank;
	}
	public void setReverseRank(Integer reverseRank) {
		this.reverseRank = reverseRank;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public Double getWeightedRating() {
		return weightedRating;
	}
	public void setWeightedRating(Double weightedRating) {
		this.weightedRating = weightedRating;
	}
	public Integer getReviews() {
		return reviews;
	}
	public void setReviews(Integer reviews) {
		this.reviews = reviews;
	}
	public String getSubRegion() {
		return subRegion;
	}
	public void setSubRegion(String subRegion) {
		this.subRegion = subRegion;
	}
	
	

}
