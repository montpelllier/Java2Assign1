import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The MovieAnalyzer class has one constructor that reads a dataset file from a given path. This
 * class also has 6 other methods that perform data analyses.
 */
public class MovieAnalyzer {

    /**
     * The Movie class stores some movie information.
     */
    public static class Movie {

        private final String seriesTitle;
        private final Integer releasedYear;
        private final String certificate;
        private final Integer runtime;
        private final List<String> genreList;
        private final Float imdbRating;
        private final String overview;
        private final Integer metaScore;
        private final String director;
        private final String[] stars;
        private final Integer noOfVotes;
        private final Integer gross;

        public String getSeriesTitle() {
            return seriesTitle;
        }

        public Integer getReleasedYear() {
            return releasedYear;
        }

        public List<String> getGenreList() {
            return genreList;
        }

        public Movie(String title, Integer year, String certificate, Integer runtime,
                List<String> genreList,
                Float rating, String overview, Integer score, String director, String[] stars,
                Integer noOfVotes, Integer gross) {
            this.seriesTitle = title;
            this.releasedYear = year;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genreList = genreList;
            this.imdbRating = rating;
            this.overview = overview;
            this.metaScore = score;
            this.director = director;
            this.stars = stars;
            this.noOfVotes = noOfVotes;
            this.gross = gross;
        }

        public String toString() {
            return String.format(
                    "Movie{Title=%s, Year=%d, Certificate=%s, Runtime=%d, Genre=%s, IMDB_Rating=%f, Meta_score=%d, Director=%s, Stars=%s, No_of_votes=%d, Gross=%d}",
                    seriesTitle, releasedYear, certificate, runtime, genreList, imdbRating,
                    metaScore,
                    director,
                    Arrays.toString(stars), noOfVotes, gross);
        }

    }

    public List<Movie> movieList;

    public static Stream<Movie> readMovies(String filename) throws IOException {
        return Files.lines(Paths.get(filename))
                .skip(1) // skip the first row
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .map(s -> {
                    Integer year = s[2].equals("") ? null : Integer.parseInt(s[2]);
                    Integer runtime =
                            s[4].equals("") ? null
                                    : Integer.parseInt(s[4].substring(0, s[4].length() - 4));
                    String genre =
                            s[5].contains("\"") ? s[5].substring(1, s[5].length() - 1) : s[5];
                    Float rating = s[6].equals("") ? null : Float.parseFloat(s[6]);
                    Integer score = s[8].equals("") ? null : Integer.parseInt(s[8]);
                    Integer noOfVotes = s[14].equals("") ? null : Integer.parseInt(s[14]);
                    Integer gross = s.length == 16 && !s[15].equals("") ? Integer.parseInt(
                            s[15].substring(1, s[15].length() - 1).replace(",", "")) : null;
                    return new Movie(s[1], year, s[3], runtime, Arrays.asList(genre.split(", ")),
                            rating, s[7], score, s[9],
                            new String[]{s[10], s[11], s[12], s[13]}, noOfVotes, gross);
                });
    }

    /**
     * The constructor of {@code MovieAnalyzer} takes the path of the dataset file and reads the
     * data. The dataset is in csv format and has the following columns: <br/> Series_Title - Name
     * of the movie <br/> Released_Year - Year at which that movie released <br/> Certificate -
     * Certificate earned by that movie <br/> Runtime - Total runtime of the movie <br/> Genre -
     * Genre of the movie <br/> IMDB_Rating - Rating of the movie at IMDB site <br/> Overview - mini
     * story/ summary <br/> Meta_score - Score earned by the movie <br/> Director - Name of the
     * Director <br/> Star1,Star2,Star3,Star4 - Name of the Stars <br/> No_of_votes - Total number
     * of votes <br/> Gross - Money earned by that movie <br/>
     *
     * @param datasetPath the path of the dataset file
     */
    public MovieAnalyzer(String datasetPath) {
        try {
            movieList = readMovies(datasetPath).toList();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method returns a {@code <year, count>} map, where the key is the year while the value is
     * the number of movies released in that year. The map should be sorted by descending order of
     * year (i.e., from the latest to the earliest).
     *
     * @return a {@code <year, count>} map
     */
    public Map<Integer, Integer> getMovieCountByYear() {
        // use TreeMap to get descending order key map
        return movieList.stream().filter(movie -> movie.getReleasedYear() != null).collect(
                        Collectors.groupingBy(Movie::getReleasedYear, TreeMap::new,
                                Collectors.summingInt(i -> 1)))
                .descendingMap();
    }

    /**
     * This method returns a {@code <genre, count>} map, where the key is the genre while the value
     * is the number of movies in that genre. The map should be sorted by descending order of count
     * (i.e., from the most frequent genre to the least frequent genre). If two genres have the same
     * count, then they should be sorted by the alphabetical order of the genre names.
     *
     * @return a {@code <genre, count>} map
     */
    public Map<String, Integer> getMovieCountByGenre() {
        // Approach 1: 先得到map，再对map进行降序排序等操作
        Map<String, Integer> unsortedMap = movieList.stream()
                .filter(movie -> movie.getGenreList() != null)
                .flatMap(movie -> movie.getGenreList().stream()).collect(
                        Collectors.groupingBy(Function.identity(), Collectors.summingInt(i -> 1)));
        return unsortedMap
                .entrySet().stream().sorted((v1, v2) -> {
                    if (v1.getValue().equals(v2.getValue())) {
                        return v1.getKey().compareTo(v2.getKey());
                    } else {
                        return v2.getValue().compareTo(v1.getValue());
                    }
                }).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (m1, m2) -> m2,
                                LinkedHashMap::new));
    }

    public Map<List<String>, Integer> getCoStarCount() {
        return null;
    }

    public List<String> getTopMovies(int topK, String by) {
        return null;
    }

    public List<String> getTopStars(int topK, String by) {
        return null;
    }

    public List<String> searchMovies(String genre, float minRating, int maxRuntime) {
        return null;
    }

}