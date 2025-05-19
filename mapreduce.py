from mrjob.job import MRJob

class MovieTags(MRJob):

    def mapper(self, _, line):
        """
        Map step: Emit movie ID as the key and tag as the value.
        Input line format: movieID,tag,timestamp
        Example: 1,funny,1139045764
        """
        try:
            movie_id, tag, timestamp = line.strip().split(',')
            yield movie_id, tag
        except ValueError:
            # Skips lines that do not match expected format
            pass

    def reducer(self, movie_id, tags):
        """
        Reduce step: Aggregate all unique tags for a movie.
        """
        unique_tags = set(tags)
        yield movie_id, list(unique_tags)

if __name__ == '__main__':
    MovieTags.run()



