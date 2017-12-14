package br.com.fasam.pos.bigdata.MoviesSearchPos;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Filme;
import br.com.fasam.pos.bigdata.MoviesSearchPos.model.MovieOfRating;
import br.com.fasam.pos.bigdata.MoviesSearchPos.repository.Filmes;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MoviesSearchPosApplicationTests {
	
	@Autowired 
	private Filmes filmes;
 
	@Test
	public void contextLoads() {
		try {
//			filmes.gravarMovieMetadata();
//			filmes.gravarMovieRatingMetadata();
			List<Filme> result = filmes.getTopFilmes();
			result.forEach(f -> System.out.println(f.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void filtrarFilmesPorNome() {
		try {
			List<Filme> result = filmes.getSearchFilmes("toy store", null, null);
			result.forEach(f -> System.out.println(f.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void filtrarFilmesPorDescricao() {
		try {
			List<Filme> result = filmes.getSearchFilmes(null, "A family wedding reignites the ancient", null);
			result.forEach(f -> System.out.println(f.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void filtrarFilmesPorAno() {
		try {
			List<Filme> result = filmes.getSearchFilmes(null, null, 1900);
			result.forEach(f -> System.out.println(f.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void pesquisarTopFilmes(){
		try {
			List<MovieOfRating> result = filmes.getTopMoviesOfRating();
			result.forEach(f -> System.out.println(f.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
