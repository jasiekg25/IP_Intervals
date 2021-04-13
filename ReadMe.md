### Recruitment Task

#### Used Technologies
<li> Scala
<li> Spark

#### Approach to the problem 
After mapping the IP values to Long values we get the problem of removing intersections from set of intervals. \
The important part of our case is that we have millions of the intervals what is more we keep them in distributed system.\
These two aspects are crucial to decide the attitude of solving the problem. I considered:
<li> Partition the intervals by the start and end values
<li> Naive solution of compering the Beginnings and Ends after previous sorting the list of intervals
<li> Use IntervalTrie data structure
<li> Since the number of possible IP address is constant we can map intervals to 1D line (variation of buckets sort)

I've decided that IntervalTrie due to insert time and that we use distributed system will be not efficient with millions of intervals.\
My choice was the last idea (bucket sort). I'm aware that we talk about the 4 billions of buckets, however the more intervals we will have the less meaningful number of buckets will be.
What is more it still has chances to be improved with proper data distribution

