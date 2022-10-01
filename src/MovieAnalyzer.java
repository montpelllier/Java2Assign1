import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
        private final String genre;
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

        public Movie(String title, Integer year, String certificate, Integer runtime, String genre,
                Float rating, String overview, Integer score, String director, String star1,
                String star2,
                String star3, String star4, Integer noOfVotes, Integer gross) {
            this.seriesTitle = title;
            this.releasedYear = year;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.imdbRating = rating;
            this.overview = overview;
            this.metaScore = score;
            this.director = director;
            this.stars = new String[]{star1, star2, star3, star4};
            this.noOfVotes = noOfVotes;
            this.gross = gross;
        }

        public String toString() {
            return String.format(
                    "Movie{Title=%s, Year=%d, Certificate=%s, Runtime=%d, Genre=%s, IMDB_Rating=%f, Meta_score=%d, Director=%s, Stars=%s, No_of_votes=%d, Gross=%d}",
                    seriesTitle, releasedYear, certificate, runtime, genre, imdbRating, metaScore,
                    director,
                    Arrays.toString(stars), noOfVotes, gross);
        }

    }

    private Stream<Movie> movieStream;

    public static Stream<Movie> readMovies(String filename) throws IOException {
        return Files.lines(Paths.get(filename))
                .skip(1)
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .map(s -> {
                    String title = s[1].substring(1, s[1].length() - 1);
                    Integer year = s[2].equals("") ? null : Integer.parseInt(s[2]);
                    Integer runtime =
                            s[4].equals("") ? null
                                    : Integer.parseInt(s[4].substring(0, s[4].length() - 4));
                    Float rating = s[6].equals("") ? null : Float.parseFloat(s[6]);
                    Integer score = s[8].equals("") ? null : Integer.parseInt(s[8]);
                    Integer noOfVotes = s[14].equals("") ? null : Integer.parseInt(s[14]);
                    Integer gross = s.length == 16 && !s[15].equals("") ? Integer.parseInt(
                            s[15].substring(1, s[15].length() - 1).replace(",", "")) : null;
                    return new Movie(title, year, s[3], runtime, s[5], rating, s[7], score, s[9],
                            s[10],
                            s[11], s[12], s[13], noOfVotes, gross);
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
            this.movieStream = readMovies(datasetPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, Integer> getMovieCountByYear() {
        return movieStream.filter(movie -> movie.getReleasedYear() != null).collect(
                        Collectors.groupingBy(Movie::getReleasedYear, TreeMap::new,
                                Collectors.summingInt(i -> 1)))
                .descendingMap();
    }

    public Map<String, Integer> getMovieCountByGenre() {
        return null;
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