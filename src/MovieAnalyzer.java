import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The MovieAnalyzer class has one constructor that reads a dataset file from a given path. This
 * class also has 6 other methods that perform data analyses.
 */
public class MovieAnalyzer {

  public List<Movie> movieList;

  /**
   * The constructor of {@code MovieAnalyzer} takes the path of the dataset file and reads the data.
   * The dataset is in csv format and has the following columns: Series_Title - Name of the movie;
   * Released_Year - Year at which that movie released; Certificate - Certificate earned by that
   * movie; Runtime - Total runtime of the movie; Genre - Genre of the movie; IMDB_Rating - Rating
   * of the movie at IMDB site; Overview - mini story / summary; Meta_score - Score earned by the
   * movie; Director - Name of the Director; Star1,Star2,Star3,Star4 - Name of the Stars;
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
          String seriesTitle = s[1].replaceAll("\"", "");
          Integer year = s[2].equals("") ? null : Integer.parseInt(s[2]);
          Integer runtime = Integer.parseInt(s[4].substring(0, s[4].length() - 4));
          String genre = s[5].replaceAll("\"", "");
          Float rating = s[6].equals("") ? null : Float.parseFloat(s[6]);
          // Note: don't handle \"\" in the overview string
          String overview =
              s[7].contains(",") || s[7].contains("\"\"") ? s[7].substring(1, s[7].length() - 1)
                  : s[7];
          Integer score = s[8].equals("") ? null : Integer.parseInt(s[8]);
          Integer noOfVotes = s[14].equals("") ? null : Integer.parseInt(s[14]);
          Integer gross = s.length == 16 && !s[15].equals("") ? Integer.parseInt(
              s[15].substring(1, s[15].length() - 1).replace(",", "")) : null;

          return new Movie(seriesTitle, year, s[3], runtime,
              Arrays.asList(genre.split(", ")),
              rating, overview, score, s[9],
              s[10], s[11], s[12], s[13], noOfVotes, gross);
        });
  }

  /**
   * A method returns a {@code <year, count>} map, where the key is the year while the value is the
   * number of movies released in that year. The map should be sorted by descending order of year
   * (i.e., from the latest to the earliest).
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

  private List<String> getCoStar(String star1, String star2) {
    List<String> stars = new ArrayList<>(2);
    //handle same name?
    /*
    if (star1.equals(star2)) {
      return null;
    }
     */

    if (star1.compareTo(star2) < 0) {
      stars.add(star1);
      stars.add(star2);
    } else {
      stars.add(star2);
      stars.add(star1);

    }
    return stars;
  }

  /**
   * If two people are the stars for the same movie, then the number of movies that they co-starred
   * increases by 1. This method returns a {@code <[star1, star2], count>} map, where the key is a
   * list of names of the stars while the value is the number of movies that they have co-starred
   * in. Note that the length of the key is 2 and the names of the stars should be sorted by
   * alphabetical order in the list.
   *
   * @return a {@code <[star1, star2], count>} map
   */
  public Map<List<String>, Integer> getCoStarCount() {
    /*
     Approach 1: get a List<Lit<String>> to present coStar by iteration first, then convert to
     stream and sort.
     */
    List<List<String>> coStarList = new ArrayList<>();
    for (Movie movie : movieList) {
      for (int i = 0; i < 4; i++) {
        for (int j = i + 1; j < 4; j++) {
          coStarList.add(getCoStar(movie.stars[i], movie.stars[j]));
        }
      }
    }
    return coStarList.stream().filter(Objects::nonNull)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(i -> 1)));
  }

  /**
   * A method returns the top K movies (parameter top_k) by the given criterion (parameter by).
   * Specifically, by="runtime": the results should be movies sorted by descending order of runtime
   * (from the longest movies to the shortest movies) . by="overview": the results should be movies
   * sorted by descending order of the length of the overview (from movies with the longest overview
   * to movies with the shortest overview). If two movies have the same runtime or overview length,
   * then they should be sorted by alphabetical order of their titles.
   *
   * @param topK The top number.
   * @param by   The given criterion.
   * @return a list of movie titles.
   */
  public List<String> getTopMovies(int topK, String by) {
    return movieList.stream().sorted((m1, m2) -> {
      switch (by) {
        case "runtime" -> {
          if (m1.runtime.equals(m2.runtime)) {
            return m1.seriesTitle.compareTo(m2.seriesTitle);
          } else {
            return m2.runtime.compareTo(m1.runtime);
          }
        }
        case "overview" -> {
          if (m1.overview.length() == m2.overview.length()) {
            return m1.seriesTitle.compareTo(m2.seriesTitle);
          } else {
            return m2.overview.length() - m1.overview.length();
          }
        }
        default -> {
          return 0;
        }
      }
    }).map(movie -> movie.seriesTitle).limit(topK).toList();
  }

  /**
   * A method returns the top K stars (parameter top_k) by the given criterion (parameter by).
   * Specifically, by="rating": the results should be stars sorted by descending order of the
   * average rating of the movies that s/he has starred in. by="gross": the results should be stars
   * sorted by descending order of the average gross of the movies that s/he has starred in. If two
   * stars have the same average rating or gross, then they should be sorted by the alphabetical
   * order of their names.
   *
   * @param topK The top number.
   * @param by   The given criterion.
   * @return a list of star names.
   */
  public List<String> getTopStars(int topK, String by) {

    Stream<Star> starStream0 = movieList.stream()
        .map(movie -> new Star(movie.stars[0], movie.imdbRating, movie.gross));
    Stream<Star> starStream1 = movieList.stream()
        .map(movie -> new Star(movie.stars[1], movie.imdbRating, movie.gross));
    Stream<Star> starStream2 = movieList.stream()
        .map(movie -> new Star(movie.stars[2], movie.imdbRating, movie.gross));
    Stream<Star> starStream3 = movieList.stream()
        .map(movie -> new Star(movie.stars[3], movie.imdbRating, movie.gross));
    Stream<Star> starStream = Stream.of(starStream0, starStream1, starStream2, starStream3)
        .flatMap(stream -> stream);
    Map<String, Double> unsortedMap;
    switch (by) {
      case "rating" -> unsortedMap = starStream.collect(
          Collectors.groupingBy(star -> star.name, Collectors.averagingDouble(Star::getRating)));
      case "gross" -> unsortedMap = starStream.filter(star -> star.getGross() != null).collect(
          Collectors.groupingBy(star -> star.name, Collectors.averagingInt(Star::getGross)));
      default -> {
        return null;
      }
    }

    return unsortedMap.entrySet().stream().sorted((s1, s2) -> {
      if (s1.getValue().equals(s2.getValue())) {
        return s1.getKey().compareTo(s2.getKey());
      } else {
        return s2.getValue().compareTo(s1.getValue());
      }
    }).map(Entry::getKey).limit(topK).toList();
  }

  public List<String> searchMovies(String genre, float minRating, int maxRuntime) {
    return null;
  }

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

    /*
    private final String star1;
    private final String star2;
    private final String star3;
    private final String star4;
     */
    private final Integer noOfVotes;
    private final Integer gross;

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
      /*
      this.star1 = star1;
      this.star2 = star2;
      this.star3 = star3;
      this.star4 = star4;
      */
      this.stars = new String[]{star1, star2, star3, star4};
      this.noOfVotes = noOfVotes;
      this.gross = gross;
    }

    public String getSeriesTitle() {
      return seriesTitle;
    }

    public Integer getReleasedYear() {
      return releasedYear;
    }

    public List<String> getGenreList() {
      return genreList;
    }

    /*
    public String getStar1() {
      return star1;
    }

    public String getStar2() {
      return star2;
    }

    public String getStar3() {
      return star3;
    }

    public String getStar4() {
      return star4;
    }
     */

    public String getOverview() {
      return overview;
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
          Arrays.toString(stars), noOfVotes, gross);
    }

  }

  public static class Star {

    private final String name;
    private final Float rating;
    private final Integer gross;

    public Star(String name, Float rating, Integer gross) {
      this.name = name;
      this.rating = rating;
      this.gross = gross;
    }

    public String getName() {
      return name;
    }

    public Float getRating() {
      return rating;
    }

    public Integer getGross() {
      return gross;
    }

  }
}