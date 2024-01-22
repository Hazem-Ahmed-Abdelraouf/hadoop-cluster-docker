import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class KmeansMR(MRJob):

    def __init__(self, *args, **kwargs):
        super(KmeansMR, self).__init__(*args, **kwargs)
        self.centroids = np.array([[4.185108184814453125e+00, -2.324152755737304688e+01],
                                    [-2.596214294433593750e+01, -1.674329948425292969e+01],
                                    [3.389326477050781250e+01, -5.574201583862304688e+00],
                                    [1.407342529296875000e+01, -2.090065002441406250e+00],
                                    [-3.468727874755859375e+01, -1.383408737182617188e+01],
                                    [2.561798477172851562e+01, -5.864925384521484375e+00]])
        self.new_centroids = np.zeros_like(self.centroids)

    def mapper(self, key, value):
        point = np.array(list(map(float, value.split())))
        distances = np.linalg.norm(self.centroids - point, axis=1)
        cluster = np.argmin(distances)
        yield int(cluster), point.tolist()

    
    def reducer(self, key, values):
        group = np.array(list(values))
        self.new_centroids[key] = group.mean(axis=0)
        yield key, self.new_centroids[key].tolist()
    
   
    def steps(self ):
        return [
            MRStep(
                   mapper=self.mapper,
                   reducer=self.reducer,
                )
        ]
    
if __name__ == "__main__":
    KmeansMR.run()
    # km =KmeansMR(args=["./points.txt", "-r", "inline"])
    # with km.make_runner() as runner:
    #     runner.run()
    #     new_centroids = [value for key, value in km.parse_output(runner.cat_output())]
    
    # print(new_centroids)


    # np.savetxt("./new_centroids.txt", np.stack(new_centroids))