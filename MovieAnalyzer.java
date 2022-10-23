import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {
    public static class Imdb {
        public String Poster_Link;
        public String Series_Title;
        public String Released_Year;
        public String Certificate;
        public String Runtime;
        public String Genre;
        public String IMDB_Rating;
        public String Overview;
        public String Meta_score;
        public String Director;
        public String Star1;
        public String Star2;
        public String Star3;
        public String Star4;
        public String Noofvotes;
        public String Gross;

        public String getPoster_Link() {
            return Poster_Link;
        }

        public String getSeries_Title() {
            String temp = Series_Title;
            if (Series_Title.charAt(0) == '\"') {
                temp = Series_Title.substring(1, Series_Title.length() - 1);
            }
            return temp;
        }

        public int getReleased_Year() {
            return Integer.parseInt(Released_Year);
        }

        public String getCertificate() {
            return Certificate;
        }

        public int getRuntime() {
            String temp = "";
            for (int i = 0; i < Runtime.length(); i++) {
                if (Runtime.charAt(i) >= 48 && Runtime.charAt(i) <= 57) {
                    temp += Runtime.charAt(i);
                }
            }
            return Integer.parseInt(temp);
        }

        public String getGenre() {
            return Genre;
        }

        public Float getIMDB_Rating() {
            return Float.parseFloat(IMDB_Rating);
        }

        public int getOverview() {
            String temp = Overview;
            if (Overview.charAt(0) == '\"') {
                temp = Overview.substring(1, Overview.length() - 1);
            }
            return temp.length();
        }

        public int getMeta_score() {
            return Integer.parseInt(Meta_score);
        }

        public String getDirector() {
            return Director;
        }

        public String getStar1() {
            return Star1;
        }

        public String getStar2() {
            return Star2;
        }

        public String getStar3() {
            return Star3;
        }

        public String getStar4() {
            return Star4;
        }

        public int getNoofvotes() {
            return Integer.parseInt(Noofvotes);
        }

        public String getGross() {
            return Gross;
        }

        public Imdb(String poster_Link, String series_Title, String released_Year, String certificate, String runtime, String genre, String imdb_rating,
                    String overview, String meta_score, String director, String star1, String star2, String star3, String star4, String noofvotes, String gross) {
            Poster_Link = poster_Link;
            Series_Title = series_Title;
            Released_Year = released_Year;
            Certificate = certificate;
            Runtime = runtime;
            Genre = genre;
            IMDB_Rating = imdb_rating;
            Overview = overview;
            Meta_score = meta_score;
            Director = director;
            Star1 = star1;
            Star2 = star2;
            Star3 = star3;
            Star4 = star4;
            Noofvotes = noofvotes;
            Gross = gross;
        }
    }

    public String dataset_path;

    public MovieAnalyzer(String dataset_path) throws IOException {
        this.dataset_path = dataset_path;
    }

    public Stream<Imdb> getStream(String dataset_path) throws IOException {
        return Files.lines(Paths.get(dataset_path).toAbsolutePath())
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .skip(1)
                .map(a -> new Imdb(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7],
                        a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15]))
                ;
    }

    public Map<Integer, Integer> getMovieCountByYear() throws IOException {
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        Map<Integer, Long> MovieCountByYear =
                movie_analyzer
                        .filter(t -> t.Released_Year != "")
                        .collect(Collectors.groupingBy(Imdb::getReleased_Year, Collectors.counting()));
        Map<Integer, Integer> res = new TreeMap<>();
        for (Map.Entry<Integer, Long> item : MovieCountByYear.entrySet()) {
            Integer year = item.getKey();
            Long count = item.getValue();
            Integer count_1 = count.intValue();
            res.put(year, count_1);
        }
        return sortKeyDescend(res);
    }

    public Map<String, Integer> getMovieCountByGenre() throws IOException {
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        Map<String, Long> MovieCountByGenre =
                movie_analyzer
                        .filter(t -> t.Genre != "")
                        .collect(Collectors.groupingBy((Imdb::getGenre), Collectors.counting()));
        Map<String, Integer> res = new HashMap<>();
        for (Map.Entry<String, Long> item : MovieCountByGenre.entrySet()) {
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
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        List<Imdb> CoStarCount =
                movie_analyzer
                        .filter(t -> (t.Star1 != "" && t.Star2 != "" && t.Star3 != "" && t.Star4 != "")).toList();
        Map<List<String>, Integer> res = new HashMap<>();
        for (Imdb curr : CoStarCount) {
            String[] stars = {curr.Star1, curr.Star2, curr.Star3, curr.Star4};
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
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        List<String> res = new ArrayList<>();
        if (by.equals("runtime")) {
            List<Imdb> TopMoviesRunTime =
                    movie_analyzer
                            .filter(t -> (t.Runtime != ""))
                            .sorted(Comparator.comparing(Imdb::getRuntime).reversed().thenComparing(Imdb::getSeries_Title)).toList();
            for (int i = 0; i < top_k; i++) {
                String curr = TopMoviesRunTime.get(i).Series_Title.replace("\"", "");
                res.add(curr);
            }
        } else {
            List<Imdb> TopMoviesOverview =
                    movie_analyzer
                            .filter(t -> (t.Overview != ""))
                            .sorted(Comparator.comparing(Imdb::getOverview).reversed().thenComparing(Imdb::getSeries_Title)).toList();
            for (int i = 0; i < top_k; i++) {
                String curr = TopMoviesOverview.get(i).Series_Title.replace("\"", "");
                res.add(curr);
            }
        }
        return res;
    }

    public List<String> getTopStars(int top_k, String by) throws IOException {
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        List<String> res = new ArrayList<>();
        if (by.equals("rating")) {
            List<Imdb> TopStarRating =
                    movie_analyzer
                            .filter(t -> (t.IMDB_Rating != "")).toList();
            Map<String, Integer> times = new HashMap<>();
            Map<String, Double> values = new HashMap<>();
            for (Imdb curr : TopStarRating) {
                String[] stars = {curr.Star1, curr.Star2, curr.Star3, curr.Star4};
                for (int i = 0; i < 4; i++) {
                    Integer time = times.get(stars[i]);
                    if (time == null) {
                        times.put(stars[i], 1);
                        values.put(stars[i], (double) Float.parseFloat(curr.IMDB_Rating));
                    } else {
                        times.put(stars[i], time + 1);
                        values.put(stars[i], (values.get(stars[i]) * time + Float.parseFloat(curr.IMDB_Rating)) / (time + 1));
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
            List<Imdb> TopStarGross =
                    movie_analyzer
                            .filter(t -> (t.Gross != "")).toList();
            Map<String, Integer> times = new HashMap<>();
            Map<String, Double> values = new HashMap<>();
            for (Imdb curr : TopStarGross) {
                String[] stars = {curr.Star1, curr.Star2, curr.Star3, curr.Star4};
                long value = Long.parseLong(curr.Gross.replace("\"", "").replace(",", ""));
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
        Stream<Imdb> movie_analyzer = getStream(dataset_path);
        List<Imdb> SearchMovies =
                movie_analyzer
                        .filter(t -> t.Genre != "")
                        .toList();
        List<String> res = new ArrayList<>();
        for (Imdb item : SearchMovies) {
            String[] gen = item.Genre.replace("\"", "").split(", ");
            for (int i = 0; i < gen.length; i++) {
                String curr = gen[i];
                if (curr.equals(genre) && item.getIMDB_Rating() >= min_rating && item.getRuntime() <= max_runtime){
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