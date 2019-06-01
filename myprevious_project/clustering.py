from IPython.display import Image
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans

import matplotlib.pyplot as plt

X, y = make_blobs(n_samples=150,
                  n_features=2,
                  centers=3,
                  cluster_std=0.5,
                  shuffle=True,
                  random_state=0)
# print(X[:, 0])
# print(X[:, 1])
# plt.scatter(X[:, 0], X[:, 1], c='red', marker='o', s=50)
# plt.grid()
# plt.tight_layout()
# #plt.savefig('./figures/spheres.png', dpi=300)
# plt.show()

km = KMeans(n_clusters=3,
            init='random',
            n_init=10,
            max_iter=300,
            tol=1e-04,
            random_state=0)
y_km = km.fit_predict(X)
print(y_km)