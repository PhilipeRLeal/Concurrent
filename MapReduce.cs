using System.Collections.Concurrent;

namespace Concurrent
{
    /// <summary>
    /// Class responsible for implementing the MapReduce policy.
    /// 
    /// Its implementation is solely based on the "System.Collections.Concurrent.Parallel.For" method. 
    /// </summary>
    public static class MapReducer
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
        ///     The conditional argument that will be used to filter the data after its being transformed 
        ///     by the <paramref name="map"/> argument. If left to default, no filtering will be applied; 
        ///     therefore, all elements of <paramref name="seq"/> will be processed. 
        ///     Otherwise, only those elements from <paramref name="seq"/> 
        ///     that return truefor the <paramref name="filter"/> will be processed and later reduced
        /// </param>
        /// 
        /// <param name="map">The mapping function that will be applied over <paramref name="seq"/> </param>
        /// 
        /// <param name="threadAggregationStepReducer">The reducer operation that will be applied individually (per thread) during the thread aggregation step.</param>
        /// 
        /// <param name="reducer">The reducer operation that will be applied over the mapped sequence. 
        /// This reducer aims to aggregate all data from all threads' partial results into a single result</param>
        /// <returns>
        ///     The list of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="reducer"/> argument.
        /// .</returns>
        public static TResult MapReduce<T, TResult>(IEnumerable<T> seq,
                                                    Func<TResult, bool>? filter,
                                                    Func<T, TResult> map,
                                                    Func<TResult, TResult, TResult> reducer,
                                                    TResult seed)
        {
            var mapped = ParallelMap.Map(seq, map, filter);

            var reduced = ParallelReduce.Reduce(mapped.AsParallel(), reducer, seed);

            return reduced;
        }

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
        /// <param name="threadAggregationStepReducer">The reducer operation that will be applied individually (per thread) during the thread aggregation step.</param>
        /// 
        /// <param name="resultAggregation">The final reducer operation that will be applied over the result. 
        /// This reducer aims to aggregate all data from all threads' partial results into a single result</param>
        /// <returns>
        ///     The list of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static List<TResult> MapReduce<T, TResult>(IEnumerable<T> seq,
                                                          Func<T, bool>? filter,
                                                          Func<T, TResult> map,
                                                          Func<List<TResult>, List<TResult>>? threadAggregationStepReducer,
                                                          Func<List<TResult>, List<TResult>>? resultAggregation)
        {
            var concurrentBag = new ConcurrentBag<TResult>();

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

                if (threadAggregationStepReducer is not null)
                {
                    reducedList = threadAggregationStepReducer(lista);
                }

                reducedList.ForEach(value =>
                {
                    lock(concurrentBag)
                    {
                        concurrentBag.Add(value);
                    }
                });
            };

            Parallel.For(0, seq.Count(), threadInit, body, threadAggregation);

            var results = new List<TResult>();

            if ( resultAggregation  is not null)
            {
                results.AddRange(resultAggregation(concurrentBag.ToList()));
            }
            else
            {
                results.AddRange(concurrentBag.ToList());
            }

            return results;
        }


        /// <summary>
        /// Executes the MapReduce operation.
        /// </summary>
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
        ///     that return true for <paramref name="filter"/> will be processed and later reduced.
        /// </param>
        /// 
        /// <param name="map">
        ///     The mapping function that will be applied over <paramref name="seq"/>
        /// </param>
        /// 
        /// <param name="reducer">
        ///     The reducer operation that will be applied over all transformed elements from seq.
        /// </param>
        /// 
        /// <returns>
        ///     The reduced operation of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static int MapReduce(IEnumerable<int> seq,
                                    Func<int, bool>? filter,
                                    Func<int, int> map,
                                    Func<int, int, int> reducer)
        {
            Func<int> threadInit = () => 0;

            var semaphore = new SemaphoreSlim(1);
            
            int result = 0;

            Action<int, ParallelLoopState> body = (i, state) =>
            {
                var elemento = seq.ElementAt(i);
                
                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        TransformAndReplace(map, reducer, elemento, ref result, semaphore);
                    }
                }
                else
                {
                    TransformAndReplace(map, reducer, elemento, ref result, semaphore);
                }
            };

            Parallel.For(0, seq.Count(), body);

            return result;
        }

        #region Private Methods

        private static void TransformAndReplace(Func<int, int> map,
                                                Func<int, int, int> reducer,
                                                int elemento,
                                                ref int result,
                                                SemaphoreSlim semaphore)
        {
            var transformed = map(elemento);

            // Using semaphore seems to be the solution here.
            semaphore.Wait();

            var reduced = reducer(result, transformed);

            var original = Interlocked.Exchange(ref result, reduced);

            semaphore.Release();
        }

        private static void TransformAndReplace(Func<double, double> map,
                                                Func<double, double, double> reducer,
                                                double elemento,
                                                ref double result,
                                                SemaphoreSlim semaphore)
        {
            var transformed = map(elemento);

            // Using semaphore seems to be the solution here.
            semaphore.Wait();

            var reduced = reducer(result, transformed);

            var original = Interlocked.Exchange(ref result, reduced);

            semaphore.Release();
        }

        private static void TransformAndReplace(Func<long, long> map,
                                                Func<long, long, long> reducer,
                                                long elemento,
                                                ref long result,
                                                SemaphoreSlim semaphore)
        {
            var transformed = map(elemento);

            // Using semaphore seems to be the solution here.
            semaphore.Wait();

            var reduced = reducer(result, transformed);

            var original = Interlocked.Exchange(ref result, reduced);

            semaphore.Release();
        }

        private static void TransformAndReplace<T>(Func<T, T> map,
                                                Func<T, T, T> reducer,
                                                T elemento,
                                                ref T result,
                                                SemaphoreSlim semaphore) where T: class
        {
            var transformed = map(elemento);

            // Using semaphore seems to be the solution here.
            semaphore.Wait();

            var reduced = reducer(result, transformed);

            var original = Interlocked.Exchange(ref result, reduced);

            semaphore.Release();
        }

        #endregion Private Methods
    }
}
