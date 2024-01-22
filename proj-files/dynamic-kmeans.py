import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from argparse import ArgumentParser
import os
class KmeansMR(MRJob):

    # def configure_args(self):
    #     super(KmeansMR, self).configure_args()
    #     self.add_passthru_arg('--iters', default=5, type=int, help='Number of iterations.')

    def __init__(self, *args, **kwargs):
        super(KmeansMR, self).__init__(*args, **kwargs)
        self.centroids = np.loadtxt(r'C:\Users\hazem\OneDrive - Universiti Sains Malaysia\Courses\Year 4\CST435 PARALLEL AND CLOUD COMPUTING\Asgn 2 - MapReduce Hadoop\centroids.txt', np.float32)
        self.new_centroids = np.zeros_like(self.centroids)

    def mapper(self, key, value):

        point = np.array(list(map(float, value.split())))
        distances = np.linalg.norm(self.centroids - point, axis=1)
        cluster = np.argmin(distances)
        yield int(cluster), point.tolist()

    
    # def reducer_init(self):
    #     self.new_centroids = np.zeros_like(self.centroids)
    
    def reducer(self, key, values):
        group = np.array(list(values))
        self.new_centroids[key] = group.mean(axis=0)
        yield key, self.new_centroids[key].tolist()
    
    # def reducer_final(self, *args, **kwargs):
    #     np.save('new_centroids.txt',self.new_centroids)

    def steps(self ):
        return [
            MRStep(
                   mapper=self.mapper,
                   reducer=self.reducer,
                #    reducer_final=self.reducer_final
                )
        ]
    
            
if __name__ == '__main__':
    argparser = ArgumentParser()
    argparser.add_argument('-i', type=str, default='points.txt', help='points path')
    argparser.add_argument('-n', type=int, default=1, help='num of iterations')
    argparser.add_argument('-o', type=str, default='centroids.txt', help='output file path')
    args = argparser.parse_args()
    # KmeansMR.run()
    for i in range(args.n):
        km =KmeansMR(args=[args.i])
        with km.make_runner() as runner:
            runner.run()
            centroids = [value for key, value in km.parse_output(runner.cat_output())]
            o = args.o
            print(centroids, o, os.getcwd(),sep='\n')
            np.savetxt(args.o, np.stack(centroids))
            

    ##find a way to feed the centroids to the class from outside 
            # mapper_init = const name for centroids
    ##so you read the points, read the centroids, calc new centroids, save them, read them again, feed them to algo and repeat