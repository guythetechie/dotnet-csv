using LanguageExt;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace csharp;

public static class EnumerableExtensions
{
    public static IEnumerable<T2> Choose<T, T2>(this IEnumerable<T> enumerable, Func<T, Option<T2>> selector) =>
        enumerable.Select(selector)
                  .Where(x => x.IsSome)
                  .Select(x => x.IfNone(() => throw new UnreachableException("All nones should have been filtered out.")));

    public static FrozenDictionary<TKey, TValue> ToFrozenDictionary<TKey, TValue>(this IEnumerable<(TKey, TValue)> enumerable, IEqualityComparer<TKey>? comparer = default) where TKey : notnull =>
        enumerable.ToFrozenDictionary(x => x.Item1, x => x.Item2, comparer);

    public static void Iter<T>(this IEnumerable<T> enumerable, Action<T> action)
    {
        foreach (var item in enumerable)
        {
            action(item);
        }
    }

    public static Option<T> Head<T>(this IEnumerable<T> enumerable)
    {
        using var enumerator = enumerable.GetEnumerator();

        return enumerator.MoveNext()
                ? enumerator.Current
                : Option<T>.None;
    }

    public static async ValueTask Iter<T>(this IEnumerable<T> enumerable, Func<T, ValueTask> action, CancellationToken cancellationToken)
    {
        var parallelOptions = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = 1
        };

        await Parallel.ForEachAsync(enumerable, parallelOptions, async (t, _) => await action(t));
    }
}

public static class DictionaryExtensions
{
    public static Option<TValue> Find<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key) =>
        dictionary.TryGetValue(key, out var value)
        ? value
        : Option<TValue>.None;
}

public static class IAsyncEnumerableExtensions
{
    public static async ValueTask<ImmutableArray<T>> ToImmutableArray<T>(this IAsyncEnumerable<T> enumerable, CancellationToken cancellationToken)
    {
        var array = await enumerable.ToArrayAsync(cancellationToken);

        return [.. array];
    }
}