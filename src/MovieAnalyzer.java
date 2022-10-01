import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {

    public static class Movie {
        private final String Series_Title;
        private final Integer Released_Year;
        private final String Certificate;
        private final Integer Runtime;
        private final String Genre;
        private final Float IMDB_Rating;
        private final String Overview;
        private final Integer Meta_score;
        private final String Director;
        private final String[] Stars;
        private final Integer No_of_votes;
        private final Integer Gross;

        public String getSeriesTitle() {return Series_Title;}
        public Integer getReleasedYear() {return Released_Year;}

        public Movie(String title, Integer year, String certificate, Integer runtime, String genre, Float rating, String overview, Integer score, String director, String star1, String star2, String star3, String star4, Integer no_of_votes, Integer gross) {
            this.Series_Title = title;
            this.Released_Year = year;
            this.Certificate = certificate;
            this.Runtime = runtime;
            this.Genre = genre;
            this.IMDB_Rating = rating;
            this.Overview = overview;
            this.Meta_score = score;
            this.Director = director;
            this.Stars = new String[]{star1, star2, star3, star4};
            this.No_of_votes = no_of_votes;
            this.Gross = gross;
        }

        public String toString() {
            return String.format("Movie{Title=%s, Year=%d, Certificate=%s, Runtime=%d, Genre=%s, IMDB_Rating=%f, Meta_score=%d, Director=%s, Stars=%s, No_of_votes=%d, Gross=%d}", Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_score, Director, Arrays.toString(Stars), No_of_votes, Gross);
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
                    Integer runtime = s[4].equals("") ? null : Integer.parseInt(s[4].substring(0, s[4].length() - 4));
                    Float rating = s[6].equals("") ? null : Float.parseFloat(s[6]);
                    Integer score = s[8].equals("") ? null : Integer.parseInt(s[8]);
                    Integer no_of_votes = s[14].equals("") ? null : Integer.parseInt(s[14]);
                    Integer gross = s.length == 16 && !s[15].equals("") ? Integer.parseInt(s[15].substring(1, s[15].length() - 1).replace(",", "")) : null;
                    return new Movie(title, year, s[3], runtime, s[5], rating, s[7], score, s[9], s[10], s[11], s[12], s[13], no_of_votes, gross);
                });
    }

    public static void main(String[] args) {

//        try {
////            Stream<String> stream = Files.lines(Paths.get("resources/imdb_top_500.csv"));
////            stream.forEach(System.out::println);
//            Stream<Movie> movieStream = Files.lines(Paths.get("resources/imdb_top_500.csv")).skip(1)
//                    .map(l -> l.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
//                    .map(s -> {
//                        //String title = s[1].substring(1, s[1].length() - 1);
//                        Short year = s[2].equals("") ? null : Short.parseShort(s[2]);
//                        Short runtime = s[4].equals("") ? null : Short.parseShort(s[4].substring(0, s[4].length() - 4));
//                        Float rating = s[6].equals("") ? null : Float.parseFloat(s[6]);
//                        Short score = s[8].equals("") ? null : Short.parseShort(s[8]);
//                        Integer no_of_votes = s[14].equals("") ? null : Integer.parseInt(s[14]);
//                        Integer gross = s.length == 16 && !s[15].equals("") ? Integer.parseInt(s[15].substring(1, s[15].length() - 1).replace(",", "")) : null;
//                        return new Movie(s[1], year, s[3], runtime, s[5], rating, s[7], score, s[9], s[10], s[11], s[12], s[13], no_of_votes, gross);
//                    });
//            movieStream.forEach(System.out::println);
//            //.map(a -> new Movie());
//        } catch (IOException ioException) {
//            ioException.printStackTrace();
//        }
    }

    /**
     * The constructor of {@code MovieAnalyzer} takes the path of the dataset file and reads the data. The dataset is in csv
     * format and has the following columns: <br/>
     * Series_Title - Name of the movie <br/>
     * Released_Year - Year at which that movie released <br/>
     * Certificate - Certificate earned by that movie <br/>
     * Runtime - Total runtime of the movie <br/>
     * Genre - Genre of the movie <br/>
     * IMDB_Rating - Rating of the movie at IMDB site <br/>
     * Overview - mini story/ summary <br/>
     * Meta_score - Score earned by the movie <br/>
     * Director - Name of the Director <br/>
     * Star1,Star2,Star3,Star4 - Name of the Stars <br/>
     * No_of_votes - Total number of votes <br/>
     * Gross - Money earned by that movie <br/>
     *
     * @param dataset_path the path of the dataset file
     */
    public MovieAnalyzer(String dataset_path) {
        try {
            this.movieStream = readMovies(dataset_path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, Integer> getMovieCountByYear() {
//        Map<Integer, Integer> map = new TreeMap<>();
//        map =
        return movieStream.filter(movie -> movie.getReleasedYear() != null).collect(Collectors.groupingBy(Movie::getReleasedYear, Collectors.summingInt(i -> 1)));
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