using System.Collections.Concurrent;

namespace Concurrent.UnitTest.Tests
{
    public static class Teste1
    {   
        [Fact]
        public static void Run()
        {

            var nTests = 1000;

            var repetitions = new List<int>[nTests];

            for ( int i = 0 ; i < nTests ; i++ )
            {
                var result = Test(0,10).OrderBy(x => x).ToList();

                repetitions[i] = result;
            }

            ValidateResults(repetitions);
        }

        static List<int> Test(int init, int final)
        {
            var result = new ConcurrentBag<int>();

            Action<int> aggregate = (value) =>
            {
                result.Add(value);
            };

            Func<int> initializer = () => 0;

            Func<int, ParallelLoopState, int, int> body = (i, state, threadAcum) =>
            {
                if (i % 2 == 0)
                {
                    threadAcum += i;
                }

                return threadAcum;
            };

            Parallel.For(init,
                         final,
                         initializer,
                         body,
                         aggregate);

            return result.ToList();
        }

        static void ValidateResults(List<int>[] repetitions)
        {
            bool testIsASuccess = true;
            string message = "";

            for ( int first = 0 ; testIsASuccess && first < repetitions.Length ; first++ )
            {
                for ( int second = 0 ; testIsASuccess && second < repetitions.Length ; second++ )
                {
                    var rep = repetitions[first];
                    var rep2 = repetitions[second];
                    if (!Enumerable.SequenceEqual(rep, rep2) )
                    {
                        Console.WriteLine("Routine 1 is not consistent");

                        var difference = rep.Except(rep2).ToList();

                        message = $"Erro durante a execução da rotina em paralelo. Os resultados processados serialmente são diferentes daqueles processados em paralelo. " +
                                  $"Ref[1]: {string.Join(',', rep)}" +
                                  $"Ref[2]: {string.Join(',', rep2)}" +
                                  $"Difereça entre sequências:{string.Join(',', difference)}";

                        Console.WriteLine(message);

                        testIsASuccess = false;
                    }
                }
            }

            // Este método não deve retornar verdadeiro
            Assert.True(testIsASuccess, message);
        }

    }
}
