import math
import random
import matplotlib.pyplot
import numpy

# define any function here!
def f(x):
    return 1.0-(1.0-0.05)**x
#math.sin(x)

# define any xmin-xmax interval here! (xmin < xmax)
xmin = 0.0
xmax = 500.0 
thex = numpy.array
they = numpy.array

# find ymin-ymax
numSteps = 10000 # bigger the better but slower!
ymin = f(xmin)
ymax = ymin
for i in range(numSteps):
    x = xmin + (xmax - xmin) * float(i) / numSteps
    y = f(x)
    if y < ymin: ymin = y
    if y > ymax: ymax = y

rectArea = (xmax - xmin) * (ymax - ymin)
numPoints = 10000
print rectArea
ctr = 0
for j in range(numPoints):
    x = xmin + (xmax - xmin) * random.random()
    y = ymin + (ymax - ymin) * random.random()
    thex=numpy.append(thex,x)
    they=numpy.append(they,y)
    if math.fabs(y) <= math.fabs(f(x)):
        if f(x) > 0 and y > 0 and y <= f(x):
            ctr += 1 # area over x-axis is positive
        if f(x) < 0 and y < 0 and y >= f(x):
            ctr -= 1 # area under x-axis is negative

fnArea = rectArea * float(ctr) / numPoints
print "Numerical integration = " + str(fnArea)
matplotlib.pyplot.scatter(x, y);
#matplotlib.pyplot.show()
matplotlib.pyplot.savefig('my1.png') 
