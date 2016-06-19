from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations

class MovieTrials(MRJob):

	def steps(self):
		return [
			MRStep(mapper = self.mapper_movieidGroup,
					reducer = reducer_movieidGroup),
			MRStep(mapper = self.mapper_create_item_pairs,
				reducer = self.reducer_compute_similarity),
			MRStep(mapper = self.mapper_sort_similarities,
				mapper_init = self.load_movie_names,
				reducer = self.reducer_output_similarities)]
			
	def mapper_movieidGroup(self, key, line):
		(userID, movieID, rating, timestamp) = line.split('\t')
		yield userID, (movieID, float(rating))


 	def reducer_movieidGroup(self, userID , itemRating):
		ratings = []
		for movieID, rating in itemRating:
			ratings.append((movieID, rating))
		yield userID, ratings

 
	def mapper_create_item_pairs(self, itemRating):

		
		for itemRating1, itemRating2 in combinations(itemRating, 2):
			ID_1 = itemRating1[0]
			rating1 = itemRating1[1]
			ID_2 = itemRating2[0]
			rating2 = itemRating2[1]

		yield (ID_1, ID_2), (rating1, rating2)
		yield (ID_2, ID_1), (rating2, rating1)
	
	def pearson_similarity(self, ratingPairs):
		numPairs = 0
		sum_xx = sum__yy = sum_xy = 0
		for ratingX, ratingY, in ratingPairs:
			sum_xx = ratingX * ratingX
			sum__yy = ratingY * ratingY
			sum_xy = ratingX * ratingY
			numPairs += 1

		numerator = float(sum_xy)
		denominator = float(sqrt(sum_xx * sum__yy))

		score = numerator / denominator

		return score, numPairs

	def reducer_compute_similarity(self, moviePair, ratingsPair):
		score, numPairs = self.pearson_similarity(ratingsPair)

		if(numPairs > 0 and score > 0.8):
			yield moviePair, (score, numPairs)

	def load_movie_names(self):
		self.movieNames = {}

		with open("u.item") as f:
			for line in f:
				fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1].decode('utf-8', 'ignore')


	def mapper_sort_similarities(self, moviePair, scores):
		score, n = scores
		movie1, movie2 = moviePair

		yield (self.movieNames[int(movie1)], score), 
			(self.movieNames[int(movie2)], n)

	def reducer_output_similarities(self, movieScore, similarNumber):
		movie1, score = movieScore
		for movie2, n in similarNumber:
			yield movie1, (movie2, score, n)



if __name__ == '__main__':
	MovieTrials.run()
