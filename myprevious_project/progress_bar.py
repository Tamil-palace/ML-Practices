import time
# print(tqdm())
# for i in tqdm(range(100)):
#     time.sleep(1)

# pbar = tqdm(total=100)
# for i in range(10):
#     pbar.update(10)
#     # time.sleep(1)
# pbar.close()

from tqdm import tnrange, tqdm_notebook
from time import sleep

for i in tnrange(10, desc='1st loop'):
    # for j in tqdm_notebook(range(100), desc='2nd loop'):
    sleep(0.01)