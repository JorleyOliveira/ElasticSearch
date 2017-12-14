package br.com.fasam.pos.bigdata.MoviesSearchPos.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class MovieOfRating implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Long userId;
	private Long movieId;
	private Double rating;
	private Long timestamp;
	private Double lat;
	private Double lng;
	private String title;
	public Long getUserId() {
		return userId;
	}
	public void setUserId(Long userId) {
		this.userId = userId;
	}
	public Long getMovieId() {
		return movieId;
	}
	public void setMovieId(Long movieId) {
		this.movieId = movieId;
	}
	public Double getRating() {
		return rating;
	}
	public void setRating(Double rating) {
		this.rating = rating;
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public Double getLng() {
		return lng;
	}
	public void setLng(Double lng) {
		this.lng = lng;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	@Override
	public String toString() {
		return "MovieOfRating [userId=" + userId + ", movieId=" + movieId
				+ ", rating=" + rating + ", timestamp=" + timestamp + ", lat="
				+ lat + ", lng=" + lng + ", title=" + title + "]";
	}
}
