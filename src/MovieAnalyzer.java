import java.util.List;
import java.util.Map;

public class MovieAnalyzer {
    /**
     * @param dataset_path
     */
    public MovieAnalyzer(String dataset_path) {

    }

    public Map<Integer, Integer> getMovieCountByYear() {
        return null;
    }

    public Map<String, Integer> getMovieCountByGenre() {
        return null;
    }

    public Map<List<String>, Integer> getCoStarCount() {
        return null;
    }

    public List<String> getTopMovies(int top_k, String by) {
        return null;
    }

    public List<String> getTopStars(int top_k, String by) {
        return null;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) {
        return null;
    }

}