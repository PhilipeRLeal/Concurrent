using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;

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
        /// <param name="threadAggregationStepReducer">The reducer operation that will be applied individually (per thread) during the thread aggregation step.</param>
        /// 
        /// <param name="resultAggregation">The final reducer operation that will be applied over the result. 
        /// This reducer aims to aggregate all data from all threads' partial results into a single result</param>
        /// <returns>
        ///     The list of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static List<TResult> Run<T, TResult>(IEnumerable<T> seq, Func<T, bool>? filter, Func<T, TResult> map, Func<List<TResult>, List<TResult>>? threadAggregationStepReducer, Func<List<TResult>, List<TResult>>? resultAggregation)
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
        /// <param name="globalReducer">The reducer operation that will be applied over all transformed elements from seq.</param>
        /// <returns>
        ///     The reduced operation of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static TResult Run<T, TResult>(IEnumerable<T> seq, Func<T, bool>? filter, Func<T, TResult> map, Func<TResult, TResult, TResult> globalReducer) where TResult : class, new()
        {
            Func<TResult> threadInit = () => new TResult();

            var result = new TResult();
            var locker = new TResult();

            Action<int, ParallelLoopState> body = (i, state) =>
            {
                var elemento = seq.ElementAt(i);

                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        var transformed = map(elemento);

                        var value = globalReducer(result, transformed);

                        Interlocked.Exchange(ref result, value);
                    }
                }
                else
                {
                    var transformed = map(elemento);

                    var value = globalReducer(result, transformed);

                    Interlocked.Exchange(ref result, value);
                }
            };

            Parallel.For(0, seq.Count(), body);

            return result;
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
        /// <param name="globalReducer">The reducer operation that will be applied over all transformed elements from seq.</param>
        /// <returns>
        ///     The reduced operation of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static int Run(IEnumerable<int> seq, Func<int, bool>? filter, Func<int, int> map, Func<int, int, int> globalReducer)
        {
            Func<int> threadInit = () => 0;
            int result = 0;

            Action<int, ParallelLoopState> body = (i, state) =>
            {
                var elemento = seq.ElementAt(i);
                
                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        result = TransformAndReplace(map, globalReducer, result, elemento);
                    }
                }
                else
                {
                    result = TransformAndReplace(map, globalReducer, result, elemento);
                }

                int TransformAndReplace(Func<int, int> map, Func<int, int, int> globalReducer, int result, int elemento)
                {
                    var transformed = map(elemento);

                    // This seems to be the problem. How to lock result, while still updating it?
                    
                    var value = globalReducer(result, transformed);

                    var original = Interlocked.Exchange(ref result, value);
                    
                    Console.WriteLine($"Original:{original} -> result: {result}");
                    return result;
                }
            };

            Parallel.For(0, seq.Count(), body);

            return result;
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
        /// <param name="globalReducer">The reducer operation that will be applied over all transformed elements from seq.</param>
        /// <returns>
        ///     The reduced operation of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static double Run(IEnumerable<double> seq, Func<double, bool>? filter, Func<double, double> map, Func<double, double, double> globalReducer)
        {
            Func<int> threadInit = () => 0;

            double result = 0;

            Action<int, ParallelLoopState> body = (i, state) =>
            {
                var elemento = seq.ElementAt(i);

                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        var transformed = map(elemento);

                        var value = globalReducer(result, transformed);

                        Interlocked.Exchange(ref result, value);
                    }
                }
                else
                {
                    var transformed = map(elemento);

                    var value = globalReducer(result, transformed);

                    Interlocked.Exchange(ref result, value);
                }
            };

            Parallel.For(0, seq.Count(), body);

            return result;
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
        /// <param name="globalReducer">The reducer operation that will be applied over all transformed elements from seq.</param>
        /// <returns>
        ///     The reduced operation of elements that are filtered by the <paramref name="filter"/> argument, later transformed by the <paramref name="map"/> argument, and lastly reduced by the <paramref name="threadAggregationStepReducer"/> argument.
        /// .</returns>
        public static long Run(IEnumerable<long> seq, Func<long, bool>? filter, Func<long, long> map, Func<long, long, long> globalReducer)
        {
            Func<int> threadInit = () => 0;

            long result = 0;

            Action<int, ParallelLoopState> body = (i, state) =>
            {
                var elemento = seq.ElementAt(i);

                if (filter is not null)
                {
                    var condition = filter(elemento);

                    if (condition)
                    {
                        var transformed = map(elemento);

                        var value = globalReducer(result, transformed);

                        Interlocked.Exchange(ref result, value);
                    }
                }
                else
                {
                    var transformed = map(elemento);

                    var value = globalReducer(result, transformed);

                    Interlocked.Exchange(ref result, value);
                }
            };

            Parallel.For(0, seq.Count(), body);

            return result;
        }
    }
}
