import sys
import numpy
import time

start = time.time()

if len(sys.argv) > 1:
  matrix_size = int(sys.argv[-1])
else:
  matrix_size = 32

print(matrix_size)

a = numpy.random.uniform(low=-1.0, high=1.0,size=(matrix_size, matrix_size))
b = numpy.random.uniform(low=-1.0, high=1.0,size=(matrix_size, matrix_size))
x = a + b

print(numpy.sum(x))

print(time.time() - start)