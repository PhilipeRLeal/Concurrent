

namespace Concurrent
{
    public static class ParallelReduce
    {
        public static T ReduceWithForImplementation<T>(this ParallelQuery<T> query, Func<T,T,T> reducer, T seed)
        {
            var result = seed;

            // Therefore, let's use Parallel.For for aggregation operations.
            var enumerable = query.ToList();

            var body = (int i, ParallelLoopState state) =>
            {
                // Would it be OK to lock on to the method itself? Because the alternative would be the result variable
                // itself, and that is being updated during this operation.
                lock(reducer)
                {
                    result = reducer(result, enumerable[i]);
                }
            };

            Parallel.For(0,
                         enumerable.Count(),
                         body);

            return result;
        }

        public static T Reduce<T>(this ParallelQuery<T> query,
                                  Func<T, T, T> reducer,
                                  T seed) 
        {
            T result = query.Aggregate(
                                         seed: seed,
                                         updateAccumulatorFunc: (acc, value) => reducer(acc, value),
                                         combineAccumulatorsFunc: (overall, local) =>
                                         reducer(overall, local),
                                         resultSelector: overall => overall);

            return result;

        }
    }
}
