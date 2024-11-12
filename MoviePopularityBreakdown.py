from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviePopularityBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie,
                   reducer=self.reducer_count_ratings_for_movie),
            MRStep(reducer=self.reducer_sort_movies_by_ratings_count)
        ]

    def mapper_get_movie(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings_for_movie(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sort_movies_by_ratings_count(self, count, movies):
        for movie in movies:
            yield rating, count


if __name__ == '__main__':
    MoviePopularityBreakdown.run()