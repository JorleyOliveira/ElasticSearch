package br.com.fasam.pos.bigdata.MoviesSearchPos.repository;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MultiMatchQuery.QueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSON;

import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Filme;
import br.com.fasam.pos.bigdata.MoviesSearchPos.model.MovieOfRating;

@Repository
public class Filmes {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private TransportClient client;
	
	private static final String INDEX_MOVIES_METADATA = "movies_metadata";

    private static final String DOC_TYPE_MOVIES = "movies";
    
    private static final String MOVIES_METADATA = "C:\\Pessoal\\SI Atual\\SI\\Pós BigData e Machine Learning\\10. Bigdata analytics\\projeto\\movies_metadata_test.csv"; 
    
    private static final String INDEX_MOVIES_RATING_METADATA = "movies_rating_metadata";
    
    private static final String DOC_TYPE_MOVIES_RATING = "movies_rating";
    
    private static final String RATING_WITH_LATLNG = "C:\\Pessoal\\SI Atual\\SI\\Pós BigData e Machine Learning\\10. Bigdata analytics\\projeto\\ratings_with_latlng_test.csv";

    
    @SuppressWarnings("resource")
	public Filmes() {
    	//TODO CONFIGURACAO ELASTIC SEARCH 6.0
    	/*
		Settings settings = Settings.builder().put("cluster.name", "my-teste").build();
		InetSocketAddress transportAddress;
		try {
			transportAddress = new InetSocketAddress(InetAddress.getByName("192.168.15.5"), 9300);
			this.client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new TransportAddress(transportAddress));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		*/
		//TODO CONFIGURACAO ELASTIC SEARCH 5.6.2
		Settings settings = Settings.builder().put("cluster.name", "my-teste").build();
		InetSocketAddress transportAddress;
		try {
			transportAddress = new InetSocketAddress(InetAddress.getByName("192.168.15.5"), 9300);
			this.client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(transportAddress));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}
    
    public void gravarMovieRatingMetadata() {
		try {
			client.prepareIndex(INDEX_MOVIES_RATING_METADATA, DOC_TYPE_MOVIES_RATING).get().getResult().toString();
		} catch (ActionRequestValidationException e) {
			try {
				Reader in = new FileReader(RATING_WITH_LATLNG);

				Scanner scan = new Scanner(new File(RATING_WITH_LATLNG));

				String header = scan.nextLine();
				String[] headerVals = header.split(",");

				try {

					Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

					BulkRequestBuilder prepareBulk = client.prepareBulk();
					int bulkCount = 0;
					for (CSVRecord record : records) {
						Map<String, Object> movie = new HashMap<>();
						String valueString = "";
                        Object value = null;
						int count = 0;
						for (String s : headerVals) {
							try {
								switch (s) {
								case "userId":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Long.valueOf(valueString.trim());
									} else {
										value = 0L;
									}
									break;
								case "movieId":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Long.valueOf(valueString.trim());
									} else {
										value = 0L;
									}
									break;
								case "rating":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Double.valueOf(valueString.trim());
									} else {
										value = 0.0D;
									}
									break;
								case "timestamp":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Long.valueOf(valueString.trim());
									} else {
										value = 0L;
									}
									break;
								case "lat":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Double.valueOf(valueString.trim());
									} else {
										value = 0.0D;
									}
									break;
								case "lng":
									valueString = record.get(count);
									if (valueString != null && ! valueString.trim().isEmpty()) {
										value = Double.valueOf(valueString.trim());
									} else {
										value = 0.0D;
									}
									break;
								default:
									count++;
                                    continue;
								}
								movie.put(s, value);
								count++;
							} catch (Exception e2) {
								break;
							}
						}
						try {
							IndexRequestBuilder source = client.prepareIndex(INDEX_MOVIES_RATING_METADATA, DOC_TYPE_MOVIES_RATING).setSource(movie);
							prepareBulk.add(source);
							bulkCount++;
							// source.get();
							if (bulkCount > 5) {
								prepareBulk.get();
								bulkCount = 0;
								prepareBulk = client.prepareBulk();
							}
						} catch (Exception e3) {
						}

					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

	}

    
	@SuppressWarnings("resource")
	public void gravarMovieMetadata() {
//		Settings settings = Settings.builder().put("cluster.name", "my-teste").build();
//		InetSocketAddress transportAddress;
		try {
//			transportAddress = new InetSocketAddress(InetAddress.getByName("192.168.15.5"), 9300);
//			this.client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(transportAddress));
			 try {
		            client.prepareIndex(INDEX_MOVIES_METADATA, DOC_TYPE_MOVIES).get().getResult().toString();
		        } catch (ActionRequestValidationException e) {
		            try {

		                File file = new File(MOVIES_METADATA);

		                Reader in = new FileReader(file);

		                Scanner scan = new Scanner(file);

		                String header = scan.nextLine();
		                String[] headerVals = header.split(",");

		                Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

		                BulkRequestBuilder prepareBulk = client.prepareBulk();		                
		                int bulkCount = 0;
		                for (CSVRecord record : records) {
		                    Map<String, Object> movie = new HashMap<>();

		                    int count = 0;
		                    for (String s : headerVals) {
		                        String valueString = "";
		                        Object value = null;
		                        try {

		                            switch (s) {
		                                case "popularity":
		                                    valueString = record.get(count);
		                                    if (valueString != null && !valueString.trim().equals("")) {
		                                        value = Double.valueOf(valueString);
		                                    }
		                                    break;
		                                case "release_date":
		                                    valueString = record.get(count);
		                                    if (valueString != null && !valueString.trim().equals("")) {
		                                        String[] valueArray = valueString.split("-");
		                                        if (valueArray.length == 3) {
		                                            value = LocalDate.of(Integer.valueOf(valueArray[0]), Integer.valueOf(valueArray[1]), Integer.valueOf(valueArray[2]));
		                                        }
		                                    }
		                                    break;
		                                case "title":
		                                    value = record.get(count);
		                                case "overview":
		                                    value = record.get(count);
		                                    break;
		                                case "id":
		                                	valueString = record.get(count);
											if (valueString != null && ! valueString.trim().isEmpty()) {
												value = Long.valueOf(valueString.trim());
											} else {
												value = 0L;
											}
		                                    break;
		                                default:
		                                    count++;
		                                    continue;
		                            }

		                        } catch (Exception e2) {
		                            String error = e2.getCause() != null ? e2.getCause().getMessage() : e2.getMessage();
		                            logger.error(String.format("Filed: %s, Value: %s, Error: %s.", s, valueString, error));
		                        }
		                        movie.put(s, value);
		                        count++;
		                    }

		                    try {
		                        IndexRequestBuilder source = client.prepareIndex(INDEX_MOVIES_METADATA, DOC_TYPE_MOVIES).setSource(movie);
		                        prepareBulk.add(source);
		                        bulkCount++;
		                        //source.get();
		                        if (bulkCount > 10) {
		                            prepareBulk.get();
		                            bulkCount = 0;
		                            prepareBulk = client.prepareBulk();
		                        }
		                    } catch (Exception e3) {
		                        logger.error(e3.getMessage(), e3);
		                    }

		                }

		            } catch (IOException e1) {
		                logger.error(e1.getMessage(), e1);
		            }
		        }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<Filme> getTopFilmes() {
		 List<Filme> filmes = new ArrayList<>();
        SearchResponse response = client.prepareSearch().addSort("popularity", SortOrder.DESC).execute().actionGet();
        List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
        searchHits.forEach(hit -> {
            filmes.add(JSON.parseObject(hit.getSourceAsString(), Filme.class));
        });
        return filmes;
	}
	
	public List<MovieOfRating> getTopMoviesOfRating() {
		try {
			List<MovieOfRating> filmes = new ArrayList<MovieOfRating>();
			SearchResponse response = client.prepareSearch(INDEX_MOVIES_RATING_METADATA)
											.addSort("rating", SortOrder.DESC)
											.execute().actionGet();
			List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
			searchHits.forEach(hit -> {
				filmes.add(JSON.parseObject(hit.getSourceAsString(), MovieOfRating.class));
			});
			filmes.forEach(f ->
			{
				String title = this.pesquisarTituloPorId(f.getMovieId());
				f.setTitle(title);
			}
			);
			return filmes;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private String pesquisarTituloPorId(Long id) {
		try {
			List<Filme> filmes = new ArrayList<>();
			SearchResponse response = client.prepareSearch()
										 	.setIndices(INDEX_MOVIES_METADATA)
										 	.setTypes(DOC_TYPE_MOVIES)
											.setQuery(QueryBuilders.termQuery("id", id))
											.execute().actionGet();
			List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
	        searchHits.forEach(hit -> {
	            filmes.add(JSON.parseObject(hit.getSourceAsString(), Filme.class));
	        });
	        if (!filmes.isEmpty()) {
	        	return filmes.get(0).getTitle();
	        } else {
	        	return "";
	        }
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public List<Filme> getSearchFilmes(String titulo, String desc, Integer ano) {
		List<Filme> filmes = new ArrayList<>();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
														 	.setIndices(INDEX_MOVIES_METADATA)
														 	.setTypes(DOC_TYPE_MOVIES); 
		if (ano != null) {
			LocalDate date = LocalDate.of(ano, Integer.valueOf(1), Integer.valueOf(1));
			searchRequestBuilder.setQuery(QueryBuilders.matchQuery("releaseDate", date));
		}
		if (titulo != null && !titulo.trim().isEmpty()){
			searchRequestBuilder.setQuery(QueryBuilders.matchQuery("title", titulo));
		}
		if (desc != null && !desc.trim().isEmpty()) {
			searchRequestBuilder.setQuery(QueryBuilders.matchPhrasePrefixQuery("overview", desc));
		}
		SearchResponse response = searchRequestBuilder.execute().actionGet(); 
		List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
        searchHits.forEach(hit -> {
            filmes.add(JSON.parseObject(hit.getSourceAsString(), Filme.class));
        });
		return filmes;
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		this.client.close();
	}
}

