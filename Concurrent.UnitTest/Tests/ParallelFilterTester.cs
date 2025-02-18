
namespace Concurrent.UnitTest.Tests
{
    /// <summary>
    /// Testa se as divisões político administrativas foram semeadas corretamente no banco de dados.
    /// </summary>
    public class ParallelFilterTester
    {
        [Fact]
        public void Testar_ParallelFilter()
        {
            int N = 100;

            var lista = Enumerable.Range(0, N);

            var filteredElements = lista.AsParallel().Filter(x => x % 2 == 1).Order();
            
            var expectedElements = Enumerable.Range(0, N).Where(x => x %2 == 1).Order().ToList();

            Assert.Equal(filteredElements.Count(), expectedElements.Count);

            Assert.Equal(filteredElements, expectedElements);
        }
    }
}
