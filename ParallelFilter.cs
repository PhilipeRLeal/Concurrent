
namespace Concurrent
{
    public static class ParallelFilter
    {
        /// <summary>
        /// Filters all elements from a sequence in parallel
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sequence"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IEnumerable<T> Filter<T>(this ParallelQuery<T> sequence, Func<T, bool> predicate)
        {
            return sequence.Aggregate(new List<T>(), 
                                      (acc, item) => {
                if ( predicate(item) )
                    acc.Add(item);
                return acc;
            });
        }

    }
}
