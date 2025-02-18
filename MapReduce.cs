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
        /// 
        /// <typeparam name="T">
        ///     The input type of <paramref name="seq"/>
        /// </typeparam>
        /// 
        /// <typeparam name="TResult">
        ///     The returned type after applying map over T
        /// </typeparam>
        /// 
        /// <param name="seq">
        ///     The sequence of elements to be processed
        /// </param>
        /// 
        /// <param name="filter">
        ///     The conditional argument that will be used to filter the data prior to its being transformed 
        ///     by the <paramref name="map"/> argument. If left to default, no filtering will be applied; 
        ///     therefore, all elements of <paramref name="seq"/> will be processed. 
        ///     Otherwise, only those elements from <paramref name="seq"/> 
        ///     that return truefor the <paramref name="filter"/> will be processed and later reduced
        /// </param>
        /// 
        /// <param name="map">The mapping function that will be applied over <paramref name="seq"/> </param>
        /// 
        /// <param name="reducer">The reducer operation that will be applied individually (per thread) during the thread aggregation step.</param>
        /// <returns>
        ///     The list of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="reducer"/> argument.
        /// .</returns>
        public static List<TResult> Run<T, TResult>(IEnumerable<T> seq, Func<T, bool>? filter, Func<T, TResult> map, Func<List<TResult>, List<TResult>>? reducer)
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
                List<TResult> reducedList = lista;

                if (reducer is not null)
                {
                    reducedList = reducer(lista);
                }

                reducedList.ForEach(value =>
                {
                    lock(results)
                    {
                        results.Add(value);
                    }
                });
            };

            Parallel.For(0, seq.Count(), threadInit, body, threadAggregation);

            return results.ToList();
        }
    }
}
