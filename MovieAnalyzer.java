import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {
    public static class Imdb {
        public String posterLink;
        public String seriesTitle;
        public String releasedYear;
        public String certificate;
        public String runtime;
        public String genre;
        public String imdbRating;
        public String overview;
        public String metaScore;
        public String director;
        public String star1;
        public String star2;
        public String star3;
        public String star4;
        public String noofVotes;
        public String gross;

        public String getPoster_Link() {
            return posterLink;
        }

        public String getSeries_Title() {
            String temp = seriesTitle;
            if (seriesTitle.charAt(0) == '\"') {
                temp = seriesTitle.substring(1, seriesTitle.length() - 1);
            }
            return temp;
        }

        public int getReleased_Year() {
            return Integer.parseInt(releasedYear);
        }

        public String getCertificate() {
            return certificate;
        }

        public int getRuntime() {
            String temp = "";
            for (int i = 0; i < runtime.length(); i++) {
                if (runtime.charAt(i) >= 48 && runtime.charAt(i) <= 57) {
                    temp += runtime.charAt(i);
                }
            }
            return Integer.parseInt(temp);
        }

        public String getGenre() {
            return genre;
        }

        public Float getImdbRating() {
            return Float.parseFloat(imdbRating);
        }

        public int getOverview() {
            String temp = overview;
            if (overview.charAt(0) == '\"') {
                temp = overview.substring(1, overview.length() - 1);
            }
            return temp.length();
        }

        public int getMeta_score() {
            return Integer.parseInt(metaScore);
        }

        public String getDirector() {
            return director;
        }

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

        public int getNoofvotes() {
            return Integer.parseInt(noofVotes);
        }

        public String getGross() {
            return gross;
        }

        public Imdb(String posterLink, String seriesTitle, String releasedYear, String certificate,
                    String runtime, String genre, String imdbRating, String overview,
                    String metaScore, String director, String star1, String star2, String star3,
                    String star4, String noofVotes, String gross) {
            this.posterLink = posterLink;
            this.seriesTitle = seriesTitle;
            this.releasedYear = releasedYear;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.imdbRating = imdbRating;
            this.overview = overview;
            this.metaScore = metaScore;
            this.director = director;
            this.star1 = star1;
            this.star2 = star2;
            this.star3 = star3;
            this.star4 = star4;
            this.noofVotes = noofVotes;
            this.gross = gross;
        }
    }

    public String datasetPath;

    public MovieAnalyzer(String datasetPath) throws IOException {
        this.datasetPath = datasetPath;
    }

    public Stream<Imdb> getStream(String datasetPath) throws IOException {
        return Files.lines(Paths.get(datasetPath).toAbsolutePath())
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .skip(1)
                .map(a -> new Imdb(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7],
                        a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15]))
                ;
    }

    public Map<Integer, Integer> getMovieCountByYear() throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        Map<Integer, Long> movieCountByYear =
                movieAnalyzer
                        .filter(t -> t.releasedYear != "")
                        .collect(Collectors.groupingBy(Imdb::getReleased_Year, Collectors.counting()));
        Map<Integer, Integer> res = new TreeMap<>();
        for (Map.Entry<Integer, Long> item : movieCountByYear.entrySet()) {
            Integer year = item.getKey();
            Long count = item.getValue();
            Integer count1 = count.intValue();
            res.put(year, count1);
        }
        return sortKeyDescend(res);
    }

    public Map<String, Integer> getMovieCountByGenre() throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        Map<String, Long> movieCountByGenre =
                movieAnalyzer
                        .filter(t -> t.genre != "")
                        .collect(Collectors.groupingBy((Imdb::getGenre), Collectors.counting()));
        Map<String, Integer> res = new HashMap<>();
        for (Map.Entry<String, Long> item : movieCountByGenre.entrySet()) {
            String[] genre = item.getKey().replace("\"", "").split(", ");
            Integer count = item.getValue().intValue();
            for (String curr : genre) {
                Integer value = res.get(curr);
                if (value == null) {
                    res.put(curr, count);
                } else {
                    value += count;
                    res.put(curr, value);
                }
            }
        }
        return sortValueDescend(res);
    }

    public Map<List<String>, Integer> getCoStarCount() throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        List<Imdb> coStarCount =
                movieAnalyzer
                        .filter(t -> (t.star1 != "" && t.star2 != "" && t.star3 != "" && t.star4 != "")).toList();
        Map<List<String>, Integer> res = new HashMap<>();
        for (Imdb curr : coStarCount) {
            String[] stars = {curr.star1, curr.star2, curr.star3, curr.star4};
            for (int i = 0; i < 3; i++) {
                for (int j = i + 1; j < 4; j++) {
                    List<String> temp = new ArrayList<>();
                    temp.add(stars[i]);
                    temp.add(stars[j]);
                    Collections.sort(temp);
                    Integer value = res.get(temp);
                    if (value == null) {
                        res.put(temp, 1);
                    } else {
                        value += 1;
                        res.put(temp, value);
                    }
                }
            }
        }
        return res;
    }

    public List<String> getTopMovies(int top_k, String by) throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        List<String> res = new ArrayList<>();
        if (by.equals("runtime")) {
            List<Imdb> topMoviesRunTime =
                    movieAnalyzer
                            .filter(t -> (t.runtime != ""))
                            .sorted(Comparator.comparing(Imdb::getRuntime).reversed().thenComparing(Imdb::getSeries_Title)).toList();
            for (int i = 0; i < top_k; i++) {
                String curr = topMoviesRunTime.get(i).seriesTitle.replace("\"", "");
                res.add(curr);
            }
        } else {
            List<Imdb> topMoviesOverview =
                    movieAnalyzer
                            .filter(t -> (t.overview != ""))
                            .sorted(Comparator.comparing(Imdb::getOverview).reversed().thenComparing(Imdb::getSeries_Title)).toList();
            for (int i = 0; i < top_k; i++) {
                String curr = topMoviesOverview.get(i).seriesTitle.replace("\"", "");
                res.add(curr);
            }
        }
        return res;
    }

    public List<String> getTopStars(int top_k, String by) throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        List<String> res = new ArrayList<>();
        if (by.equals("rating")) {
            List<Imdb> topStarRating =
                    movieAnalyzer
                            .filter(t -> (t.imdbRating != "")).toList();
            Map<String, Integer> times = new HashMap<>();
            Map<String, Double> values = new HashMap<>();
            for (Imdb curr : topStarRating) {
                String[] stars = {curr.star1, curr.star2, curr.star3, curr.star4};
                for (int i = 0; i < 4; i++) {
                    Integer time = times.get(stars[i]);
                    if (time == null) {
                        times.put(stars[i], 1);
                        values.put(stars[i], (double) Float.parseFloat(curr.imdbRating));
                    } else {
                        times.put(stars[i], time + 1);
                        values.put(stars[i], (values.get(stars[i]) * time + Float.parseFloat(curr.imdbRating)) / (time + 1));
                    }
                }
            }
            Map<String, Double> result = sortValueDescend(values);
            int i = 0;
            for (Map.Entry<String, Double> item : result.entrySet()) {
                if (i == top_k) {
                    break;
                }
                res.add(item.getKey());
                i++;
            }
        } else {
            List<Imdb> topStarGross =
                    movieAnalyzer
                            .filter(t -> (t.gross != "")).toList();
            Map<String, Integer> times = new HashMap<>();
            Map<String, Double> values = new HashMap<>();
            for (Imdb curr : topStarGross) {
                String[] stars = {curr.star1, curr.star2, curr.star3, curr.star4};
                long value = Long.parseLong(curr.gross.replace("\"", "").replace(",", ""));
                for (int i = 0; i < 4; i++) {
                    Integer time = times.get(stars[i]);
                    if (time == null) {
                        times.put(stars[i], 1);
                        values.put(stars[i], (double) value);
                    } else {
                        times.put(stars[i], time + 1);
                        values.put(stars[i], (values.get(stars[i]) * time + value) / (time + 1));
                    }
                }
            }
            Map<String, Double> result = sortValueDescend(values);
            int i = 0;
            for (Map.Entry<String, Double> item : result.entrySet()) {
                if (i == top_k) {
                    break;
                }
                res.add(item.getKey());
                i++;
            }
        }
        return res;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) throws IOException {
        Stream<Imdb> movieAnalyzer = getStream(datasetPath);
        List<Imdb> searchMovies =
                movieAnalyzer
                        .filter(t -> t.genre != "")
                        .toList();
        List<String> res = new ArrayList<>();
        for (Imdb item : searchMovies) {
            String[] gen = item.genre.replace("\"", "").split(", ");
            for (int i = 0; i < gen.length; i++) {
                String curr = gen[i];
                if (curr.equals(genre) && item.getImdbRating() >= min_rating && item.getRuntime() <= max_runtime) {
                    res.add(item.getSeries_Title());
                }
            }
        }
        Collections.sort(res);
        return res;
    }

    public static <K extends Comparable<? super K>, V> Map<K, V> sortKeyDescend(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int compare = (o1.getKey()).compareTo(o2.getKey());
                return -compare;
            }
        });

        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }

    public static <K extends Comparable<? super K>, V extends Comparable<? super V>> Map<K, V> sortValueDescend(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int compare = (o1.getValue()).compareTo(o2.getValue());
                if (compare == 0) {
                    return (o1.getKey()).compareTo(o2.getKey());
                }
                return -compare;
            }
        });

        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }
}