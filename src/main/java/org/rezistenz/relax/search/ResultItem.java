package org.rezistenz.relax.search;

/**
 * Класс предназначенный для хранения и передачи
 * данных ответа сервиса. 
 * 
 * @author alex
 *
 */
public class ResultItem {
	
	private String name;
	private String address;
	private double rating;
	
	public ResultItem(String name, String address, double rating) {
		super();
		this.name = name;
		this.address = address;
		this.rating = rating;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getAddress() {
		return address;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public double getRating() {
		return rating;
	}
	
	public void setRating(double rating) {
		this.rating = rating;
	}
}
