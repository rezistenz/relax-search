package org.rezistenz.relax.search;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.base.Stopwatch;

/**
 * Класс, предназначенный для выполнеия задачи поиска 
 * самой популярной фирмы с заполенным рейтенгом на фламп, 
 * по критерию поиска и месту нахождения.
 * 
 * Предназначен для выполнениия в отдельном потоке.
 * 
 * @author alex
 *
 */
public class RelaxSearchTaskAsync implements Callable<ResultItem> {

	private static final String FIRM_SEARCH_SERVICE_URL = "http://catalog.api.2gis.ru/search";
	private static final String FIRM_PROFILE_SERVICE_URL = "http://catalog.api.2gis.ru/profile";
	
	private static final Logger log = LoggerFactory.getLogger(RelaxSearchTaskAsync.class);
	
	/**
	 * Коструктор, для создания задачи поиска.
	 * 
	 * @param what - критерий поиска
	 * @param where - место нахождения
	 * @param key - ключ для доступа к удаленным сервисам
	 */
	public RelaxSearchTaskAsync(String what, String where, String key) {
		this.what = what;
		this.where = where;
		this.key = key;
	}
	
	private String what;
	private String where;
	private String key;
	
	@Override
	public ResultItem call() throws Exception {
		return getResultItemWithMaxRating();
	}

	/**
	 * Возвращает результат выполнения задачи, - 
	 * описание самой популярную фирмы с заполенным рейтенгом на фламп, 
	 * по критерию поиска и месту нахождения, либо null если ничего не найдено.
	 * 
	 * @param what - критерий поиска
	 * @param where -  место нахождения
	 * 
	 * @return ResultItem - описание фирмы, либо null если ничего не найдено.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private ResultItem getResultItemWithMaxRating() throws InterruptedException, ExecutionException {
		Stopwatch stopwatch = Stopwatch.createStarted();
		
		ResultItem resultItem = null;
		
		Client client = ClientBuilder.newClient();
		
		WebTarget target = client.target(FIRM_SEARCH_SERVICE_URL)
			.queryParam("version", "1.3")
			.queryParam("key", key)
			.queryParam("what", what)
			.queryParam("where", where)
			.queryParam("sort", "rating")
			.queryParam("pagesize","20")
			.queryParam("page", "1");
		
		Future<JsonObject> firmSearchFuture = target.request(MediaType.APPLICATION_JSON_TYPE)
			.async().get(JsonObject.class);
		
		JsonObject jsonObject = firmSearchFuture.get();
		
		client.close();
		
		if(jsonObject.containsKey("result")){
			JsonArray jsonArray = jsonObject.getJsonArray("result");
			
			if(!jsonArray.isEmpty()){
				for (JsonValue jsonValue : jsonArray) {
					JsonObject item = (JsonObject) jsonValue;
					
					//only if reviews_count is set
					if(item.containsKey("reviews_count")){
						int reviews_count = item.getInt("reviews_count");
						
						log.info("reviews_count: " + reviews_count);
						
						String id = item.getString("id");
						
						double rating = buildRating(id);
						
						if(rating > 0.0D){
							resultItem = new ResultItem(
									item.getString("name"), 
									where + ", " + item.getString("address"), 
									rating
								);
							
							//founded only first reviewed firm with not void rating
							break;
						}
					}
				}//end for
			}
		}
		
		stopwatch.stop();
		
		log.info("Task execution time: " + stopwatch);
		
		return resultItem;
	}

	/**
	 * Получение рейтинга фирмы на фламп по идентификатору. 
	 * Если у фирмы нет рейтенга то возвращается константа 0.0D.
	 * 
	 * @param id - идентификатор фирмы
	 * 
	 * @return Значение рейтинга, либо 0.0D если значение рейтенга не задано.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private double buildRating(String id) throws InterruptedException, ExecutionException {
		double result = 0.0D;
		
		Client client = ClientBuilder.newClient();
		
		WebTarget target = client.target(FIRM_PROFILE_SERVICE_URL)
			.queryParam("version", "1.3")
			.queryParam("key", key)
			.queryParam("id", id);
			
		Future<JsonObject> profileFuture = target.request(MediaType.APPLICATION_JSON_TYPE)
				.async().get(JsonObject.class);
		
		JsonObject jsonObject = profileFuture.get();
		
		client.close();
		
		//only if rating is set
		if(jsonObject.containsKey("rating")){
			String ratingString = jsonObject.getString("rating");
			
			log.info("rating: " + ratingString);
			
			try{
				result = Double.parseDouble(ratingString);
			}catch(Exception e){
				log.error(e.getMessage(), e);
			}
		}
		
		return result;
	}
	
}
