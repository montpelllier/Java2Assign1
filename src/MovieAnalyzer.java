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
        //        private final String[] stars;
        private final String star1;
        private final String star2;
        private final String star3;
        private final String star4;

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

        /**
         * The constructor of Movie.
         *
         * @param title       Name of the movie.
         * @param year        Year at which that movie released.
         * @param certificate Certificate earned by that movie.
         * @param runtime     Total runtime of the movie.
         * @param genreList   Genre of the movie.
         * @param rating      Rating of the movie at IMDB site.
         * @param overview    mini story / summary.
         * @param score       Score earned by the movie.
         * @param director    Name of the Director.
         * @param star1       Name of the Stars.
         * @param star2       Name of the Stars.
         * @param star3       Name of the Stars.
         * @param star4       Name of the Stars.
         * @param noOfVotes   Total number of votes.
         * @param gross       Money earned by that movie.
         */

        public Movie(String title, Integer year, String certificate, Integer runtime,
                List<String> genreList,
                Float rating, String overview, Integer score, String director, String star1,
                String star2, String star3, String star4,
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
            this.star1 = star1;
            this.star2 = star2;
            this.star3 = star3;
            this.star4 = star4;
            this.noOfVotes = noOfVotes;
            this.gross = gross;
        }

        /**
         * Returns a string representation of all the values.
         *
         * @return a string representation of all the values.
         */
        public String toString() {
            return String.format(
                    "Movie{Title=%s, Year=%d, Certificate=%s, Runtime=%d, Genre=%s, IMDB_Rating=%f"
                            + ", Meta_score=%d, Director=%s, Stars=%s, No_of_votes=%d, Gross=%d}",
                    seriesTitle, releasedYear, certificate, runtime, genreList, imdbRating,
                    metaScore,
                    director,
                    Arrays.toString(new String[]{star1, star2, star3, star4}), noOfVotes, gross);
        }

    }

    public List<Movie> movieList;

    /**
     * Parse the csv file into Movie stream.
     *
     * @param filename The file path name.
     * @return a stream of Movie.
     * @throws IOException if the filename is not correct.
     */
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
                            s[10], s[11], s[12], s[13], noOfVotes, gross);
                });
    }

    /**
     * The constructor of {@code MovieAnalyzer} takes the path of the dataset file and reads the
     * data. The dataset is in csv format and has the following columns: Series_Title - Name of the
     * movie; Released_Year - Year at which that movie released; Certificate - Certificate earned by
     * that movie; Runtime - Total runtime of the movie; Genre - Genre of the movie; IMDB_Rating -
     * Rating of the movie at IMDB site; Overview - mini story / summary; Meta_score - Score earned
     * by the movie; Director - Name of the Director; Star1,Star2,Star3,Star4 - Name of the Stars;
     * No_of_votes - Total number of votes; Gross - Money earned by that movie.
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
     * A method returns a {@code <year, count>} map, where the key is the year while the value is
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
     * A method returns a {@code <genre, count>} map, where the key is the genre while the value is
     * the number of movies in that genre. The map should be sorted by descending order of count
     * (i.e., from the most frequent genre to the least frequent genre). If two genres have the same
     * count, then they should be sorted by the alphabetical order of the genre names.
     *
     * @return a {@code <genre, count>} map
     */
    public Map<String, Integer> getMovieCountByGenre() {
        // Approach 1: 先合并List<String>的stream得到未排序的map，再对map进行降序排序等操作
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

    /**
     * If two people are the stars for the same movie, then the number of movies that they
     * co-starred increases by 1. This method returns a {@code <[star1, star2], count>} map, where
     * the key is a list of names of the stars while the value is the number of movies that they
     * have co-starred in. Note that the length of the key is 2 and the names of the stars should be
     * sorted by alphabetical order in the list.
     *
     * @return a {@code <[star1, star2], count>} map
     */
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