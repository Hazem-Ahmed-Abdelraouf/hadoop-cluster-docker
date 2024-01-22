import math, matplotlib.pyplot as plt, operator, torch
import numpy as np
from functools import partial
from torch.distributions.multivariate_normal import MultivariateNormal
from torch import tensor
from mrjob.job import MRJob
from mrjob.step import MRStep


n_clusters=6
n_samples =250
torch.manual_seed(5)

def create_data(n_clusters, n_samples):
    centroids = torch.rand(n_clusters, 2)*70-35
    def sample(m): return MultivariateNormal(m, torch.diag(tensor([5.,5.]))).sample((n_samples,))
    slices = [sample(c) for c in centroids]
    data = torch.cat(slices)
    return centroids, data

centroids, data = create_data(n_clusters, n_samples)

print(data.shape, centroids.shape)
def plot_data(centroids, data, n_samples, ax=None):
    if ax is None: _,ax = plt.subplots()
    for i, centroid in enumerate(centroids):
        samples = data[i*n_samples:(i+1)*n_samples]
        ax.scatter(samples[:,0], samples[:,1], s=1)
        ax.plot(*centroid, markersize=10, marker="x", color='k', mew=5)
        ax.plot(*centroid, markersize=5, marker="x", color='m', mew=2)
    plt.show()

centroids = torch.rand(n_clusters, 2)*70-35
plot_data(centroids, data, n_samples)

np.savetxt('./points.txt', data)
np.savetxt('./centroids.txt', centroids)
# class MRKmeans(MRJob):
