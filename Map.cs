using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Concurrent
{
    public static class ParallelMap
    {
        
        /// <summary>
        /// Applies a transformation operation over all elements of a sequence in parallel and 
        /// returns those elements that are valid for the provided predicate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="R"></typeparam>
        /// <param name="sequence"></param>
        /// <param name="map"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IEnumerable<R> Map<T, R> (IEnumerable<T> sequence, Func<T,R> map, Func<R, bool>? predicate = null)
        {

            return sequence.AsParallel().Aggregate(new List<R>(), (acc, item) =>
            {
                var response = map(item);

                if ( predicate is not null && 
                     predicate(response) )
                {
                    acc.Add(response);
                }

                return acc;
            });

        }

    }
}
