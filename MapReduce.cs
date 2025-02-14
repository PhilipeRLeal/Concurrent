using System.Collections.Concurrent;

namespace Concurrent
{
    /// <summary>
    /// Class responsible for implementing the MapReduce policy.
    /// 
    /// Its implementation is solely based on the "System.Collections.Concurrent.Parallel.For" method. 
    /// </summary>
    public static class MapReduce
    {
        /// <summary>
        /// Executes the MapReduce operation.
        /// </summary>
        /// <typeparam name="T">The input type of <paramref name="seq"/></typeparam>
        /// <typeparam name="TResult">The returned type after applying map over T</typeparam>
        /// <param name="seq">The sequence of elements to be processed</param>
        /// <param name="map">The mapping function that will be applied over <paramref name="seq"/> </param>
        /// <param name="filter">The conditional that will be used to accumulate the mapped data. 
        /// If default, no filtering will be applied; 
        /// therefore, all elements of <paramref name="seq"/> will be processed. 
        /// Otherwise, only those elements from <paramref name="seq"/> that return true 
        /// for the <paramref name="filter"/> will be processed and later returned by this method</param>
        /// <returns>The list of elements that were filtered by the <paramref name="filter"/> method and later transformed by the <paramref name="map"/> function.</returns>
        public static List<TResult> Run<T, TResult>(IEnumerable<T> seq, Func<T, TResult> map, Func<T, bool>? filter = null)
        {
            var results = new ConcurrentBag<TResult>();

            Func<List<TResult>> threadInit = () => new List<TResult>();

            Func<int, ParallelLoopState, List<TResult>, List<TResult>> body = (i, state, lista) =>
            {
                var elemento = seq.ElementAt(i);

                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        var transformed = map(elemento);
                        lista.Add(transformed);
                    }
                }
                else
                {
                    var transformed = map(elemento);
                    lista.Add(transformed);
                }
                

                return lista;
            };

            Action<List<TResult>> threadAggregation = (lista) =>
            {
                lista.ForEach(valor =>
                {
                    lock(results)
                    {
                        results.Add(valor);
                    }
                });
            };

            Parallel.For(0, seq.Count(), threadInit, body, threadAggregation);

            return results.ToList();
        }
    }
}
