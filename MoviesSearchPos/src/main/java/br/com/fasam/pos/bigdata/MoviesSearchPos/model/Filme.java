package br.com.fasam.pos.bigdata.MoviesSearchPos.model;

import java.io.Serializable;
import java.time.LocalDate;

import lombok.Data;

@Data
public class Filme implements Serializable{
	private static final long serialVersionUID = 2399028824723797630L;

	private String title;
    private String overview;
    private LocalDate releaseDate;
    private Double popularity;
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getOverview() {
		return overview;
	}
	public void setOverview(String overview) {
		this.overview = overview;
	}
	public LocalDate getReleaseDate() {
		return releaseDate;
	}
	public void setReleaseDate(LocalDate releaseDate) {
		this.releaseDate = releaseDate;
	}
	public Double getPopularity() {
		return popularity;
	}
	public void setPopularity(Double popularity) {
		this.popularity = popularity;
	}
	@Override
	public String toString() {
		return "Filme [title=" + title + ", overview=" + overview
				+ ", releaseDate=" + releaseDate + ", popularity=" + popularity
				+ "]";
	}
    
}
