﻿using System.Collections.Concurrent;

namespace Concurrent.UnitTest.Tests
{
    public static class Teste3
    {
        [Fact]
        public static void Run()
        {

            var nTests = 10_000;

            var repetitions = new List<int>[nTests];

            for ( int i = 0 ; i < nTests ; i++ )
            {
                var result = Test(0,10, i);

                repetitions[i] = result;
            }

            ValidateResults(repetitions);
        }

        static List<int> Test(int init, int final, int iteration)
        {
            var result = new ConcurrentBag<int>();

            Action<int> aggregate = (value) =>
            {
                lock(result)
                {
                    if (!result.Contains(value))
                    {
                        if (value % 2 == 0)
                        {
                            result.Add(value);
                        }
                    }
                }
            };

            Func<int> initializer = () => 0;

            Func<int, ParallelLoopState, int, int> body = (i, state, threadAcum) =>
            {
                return i;
            };

            Parallel.For(init,
                         final,
                         initializer,
                         body,
                         aggregate);

            // Console.WriteLine($"Iteration:{iteration}");

            return result.ToList();
        }

        static void ValidateResults(List<int>[] repetitions)
        {
            bool testIsASuccess = true;

            for ( int first = 0 ; testIsASuccess && first < repetitions.Length ; first++ )
            {
                for ( int second = 0 ; testIsASuccess && second < repetitions.Length ; second++ )
                {
                    var rep = repetitions[first];
                    var rep2 = repetitions[second];
                    if ( !Enumerable.SequenceEqual(rep.OrderBy(x => x), rep2.OrderBy(x => x)) )
                    {
                        Assert.Fail("Routine 3 is not consistent");

                        //Console.WriteLine($"rep[{first}]:{string.Join(',', rep)}");
                        //Console.WriteLine($"rep[{second}]:{string.Join(',', rep2)}");

                        testIsASuccess = false;
                    }
                }
            }

            // Este método não deve retornar verdadeiro
            Assert.False(testIsASuccess);
        }

    }
}
