# import re
# list_len=["ml","amount","breadth","capacity","content","diameter","extent","height","intensity","length","magnitude","proportion","range","scope","stature","volume","width","amplitude","area","bigness","body","caliber","capaciousness","dimensions","enormity","extension","greatness","highness","immensity","largeness","mass","measurement","proportions","spread","stretch","substance","substantiality","tonnage","vastness","voluminosity","admeasurement","hugeness"]
# print(len(list_len))
# text="LiveFresh Mesh Strainer Stainless Set - Premium Fine Stainless Steel Fine Mesh Strainers, Colanders and Sifters Crafted for Quinoa & Amaranth with Comfortable Non Slip Handles - 3 Sizes".lower()
# print(len(text))
# for color in list_len:
#     text=re.sub(r"\s+"+str(color)+"\s+","",text,re.I)
#     # text=re.sub(r"\s+\d+\s+","",text,re.I)
#     text=re.sub(r"\d+","",text,re.I)
# print(len(text))
# print(text)
# import heapq
# el = [20,67,3,2.6,7,74,2.8,90.8,52.8,4,3,2,5,7]
# print(heapq.nlargest(2, el))
# print(sorted(el)[-2])

# Importing gc module
import gc

# Returns the number of
# objects it has collected
# and deallocated
collected = gc.collect()

# Prints Garbage collector
# as 0 object
# import gc
#
# i = 0
#
#
# # create a cycle and on each iteration x as a dictionary
# # assigned to 1
# def create_cycle():
#     x = {}
#     x[i + 1] = x
#     print
#     x
#
#
# # lists are cleared whenever a full collection or
# # collection of the highest generation (2) is run
# collected = gc.collect()  # or gc.collect(2)
# print("Garbage collector: collected %d objects." % (collected))
#
# print("Creating cycles...")
# for i in range(10):
#     create_cycle()
#
# collected = gc.collect()
#
# print("Garbage collector: collected %d objects." % (collected))

import sys, gc

def make_cycle():
1 = { }
1[0] = 1

def main():
collected = gc.collect()
print "Garbage collector: collected %d objects." % (collected)
print "Creating cycles..."
for i in range(10):
make_cycle()
collected = gc.collect()
print("Garbage collector: collected %d objects." % (collected))

if __name__ == "__main__":
ret = main()
sys.exit(ret)