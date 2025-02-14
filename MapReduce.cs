using System.Collections.Concurrent;

namespace Concurrent
{
    public static class MapReduce
    {
        public static List<TResult> Run<T, TResult>(IEnumerable<T> seq, Func<T, TResult> map, Func<T, bool> reducerCondition)
        {
            var results = new ConcurrentBag<TResult>();

            Func<List<TResult>> threadInit = () => new List<TResult>();

            Func<int, ParallelLoopState, List<TResult>, List<TResult>> body = (i, state, lista) =>
            {
                var elemento = seq.ElementAt(i);

                var condition = reducerCondition(elemento);

                if (condition)
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
