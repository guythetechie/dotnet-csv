using LanguageExt;
using System;
using System.Threading.Tasks;

namespace csharp;

internal static class OptionExtensions
{
    public static async ValueTask IterTask<T>(this Option<T> option, Func<T, ValueTask> action) =>
        await option.Match(action, () => ValueTask.CompletedTask);
}
