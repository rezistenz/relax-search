package org.rezistenz.relax.search;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.base.Stopwatch;

/**
 * Класс предназначенный для обраобки запросов сервиса поиска наиболее популярных фирм.
 * Публикует http метод /search, 
 * предназначенный для обработки запросов на поиск.
 * 
 * @author alex
 *
 */
@Path("/")
public class RelaxSearchService {
	
	/**
	 * Свойство, содержащее список мест(городов), прецисленных через раделитель(',' - запятая).
	 */
	private static final String WHERES_PROP = "org.rezistenz.relax.search.RelaxSearchService.wheres";
	/**
	 * Свойство, содержащее ключ для доступа к внешним сервисам.
	 */
	private static final String KEY_PROP = "org.rezistenz.relax.search.RelaxSearchService.key";
	
	private static final Logger log = LoggerFactory.getLogger(RelaxSearchService.class);
	
	/**
	 * Пулл потоков для выплоения задач поиска.
	 */
	private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
	
	/**
	 * Список мест(городов), по которым осуществлять поиск.
	 * Инициализируется на этапе загрузки класса из файла свойств.
	 */
	private static final List<String> WHERES;
	/**
	 * Ключ для доспупа к внешним сервисам поиска.
	 * Инициализируется на этапе загрузки класса из файла свойств.
	 */
	private static final String KEY_VAlUE;
	
	static {
		Properties prop = new Properties();
		
		try {
			prop.load(
					RelaxSearchService.class.getResourceAsStream("/config.properties")
				);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		
		WHERES = initWheres(prop);
		KEY_VAlUE = prop.getProperty(KEY_PROP, "");
	}
	
	private static List<String> initWheres(Properties prop){
		List<String> result = new LinkedList<String>();
		
		String wheresString = prop.getProperty(WHERES_PROP, "");
		
		if(!wheresString.isEmpty()){
			String[] wheres = wheresString.split(",");
			
			for (String where : wheres) {
				result.add(where);
			}
		}
		
		log.info(result.toString());
		
		return result;
	}
	
	/**
	 * Метод поиска наиболее поуляных фирм с сортирокой по рейтингу фламп.
	 * 
	 * @param what - параметр(критерий) поиска (например кино, театр, пицца).
	 * 
	 * @return json представление списка результатов поиска, 
	 * либо статус ошибки(http код 400) если критерий поиска пустой.
	 */
	@GET
	@Path("/search")
	@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
	public Response search(
			@DefaultValue("") @QueryParam("what") String what){
		Status status = Status.OK;
		
		Object result = null;
		
		if(!what.isEmpty()){
			result = buildResult(what);
		}else{
			status = Status.BAD_REQUEST;
		}
		
		return Response.status(status).entity(result).build();
	}
	
	/**
	 * Основной метод для построения результата.
	 * Поиск результатов осуществляестя по местам(городам), 
	 * перечисленных в списке {@link #WHERES}. 
	 * Обращение к удаленным сервисам идет в нескольких потоках.
	 * 
	 * @param what - критерий поиска
	 * 
	 * @return Список результаов, 
	 * отсортированных по рейтенгу на фламп в убывающейм порядке.
	 */
	private List<ResultItem> buildResult(String what) {
		Stopwatch stopwatch = Stopwatch.createStarted();
		
		List<ResultItem> result = new LinkedList<ResultItem>();
		
		List<Future<ResultItem>> tasks = new LinkedList<Future<ResultItem>>();
		
		for (String where : WHERES) {
			Future<ResultItem> task = EXECUTOR_SERVICE.submit(
					new RelaxSearchTaskAsync(
							what, 
							where, 
							KEY_VAlUE
						)
				);
			tasks.add(task);
		}
		
		for (Future<ResultItem> task : tasks) {
			try {
				ResultItem resultItem = task.get();
				
				if(resultItem != null){
					result.add(resultItem);
				}
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				log.error(e.getMessage(), e);
			}
		}
		
		log.info("LargestPoolSize: " + ((ThreadPoolExecutor)EXECUTOR_SERVICE).getLargestPoolSize() );
		log.info("PoolSize: " + ((ThreadPoolExecutor)EXECUTOR_SERVICE).getPoolSize() );
		
		Collections.sort(result, new Comparator<ResultItem>() {
			@Override
			public int compare(ResultItem o1, ResultItem o2) {
				if(o1.getRating() < o2.getRating()){
					return 1;
				}else if(o1.getRating() > o2.getRating()){
					return -1;
				}
				return 0;
			}
		});
		
		log.info("Execution time: " + stopwatch.stop());
		
		return result;
	}
	
}
