using FluentAssertions;
using FluentAssertions.Execution;
using FluentAssertions.Primitives;
using LanguageExt;
using LanguageExt.UnsafeValueAccess;

namespace csharp.tests;

internal static class OptionExtensions
{
    public static OptionAssertions<T> Should<T>(this Option<T> instance) where T : notnull =>
        new OptionAssertions<T>(instance);
}

internal sealed class OptionAssertions<T>(Option<T> subject) : ReferenceTypeAssertions<Option<T>, OptionAssertions<T>>(subject) where T : notnull
{
    protected override string Identifier { get; } = "option";

    [CustomAssertion]
    public AndConstraint<OptionAssertions<T>> BeSome(string because = "", params object[] becauseArgs)
    {
        Execute.Assertion
               .BecauseOf(because, becauseArgs)
               .ForCondition(Subject.IsSome)
               .FailWith("Expected {context:option} to be Some{reason}, but it is None.");

        return new AndConstraint<OptionAssertions<T>>(this);
    }

    [CustomAssertion]
    public AndConstraint<OptionAssertions<T>> BeSome(T expected, string because = "", params object[] becauseArgs)
    {
        Execute.Assertion
               .BecauseOf(because, becauseArgs)
               .WithExpectation("Expected {context:option} to be Some {0}{reason}, ", expected)
               .ForCondition(Subject.IsSome)
               .FailWith("but it is None.")
               .Then
               .Given(() => Subject.ValueUnsafe())
               .ForCondition(actual => expected.Equals(actual))
               .FailWith("but it is {0}.", t => new[] { t });

        return new AndConstraint<OptionAssertions<T>>(this);
    }

    [CustomAssertion]
    public AndConstraint<OptionAssertions<T>> BeNone(string because = "", params object[] becauseArgs)
    {
        Execute.Assertion
               .BecauseOf(because, becauseArgs)
               .ForCondition(Subject.IsNone)
               .FailWith("Expected {context:option} to be None{reason}, but it is Some.");

        return new AndConstraint<OptionAssertions<T>>(this);
    }
}

//internal class OptionAsserations<T>(Option<T> instance) : ReferenceTypeAssertions<Option<T>, OptionAssertions<T>>(instance)
//{
//    protected override string Identifier => "option";

//    public AndConstraint<OptionAssertions<T>> BeNone(string because = "", params object[] becauseArgs)
//    {
//        Execute.Assertion
//            .BecauseOf(because, becauseArgs)
//            .WithExpectation("Expected {context:option} to be None{reason}, ")
//            .Given(() => Subject)
//            .ForCondition(subject => subject.IsNone)
//            .FailWith("but found to be Some.");

//        return new AndConstraint<OptionAssertions<T>>(this);
//    }

//    public AndWhichConstraint<OptionAssertions<T>, T> BeSome(string because = "", params object[] becauseArgs)
//    {
//        Execute.Assertion
//            .BecauseOf(because, becauseArgs)
//            .WithExpectation("Expected {context:option} to be Some{reason}, ")
//            .Given(() => Subject)
//            .ForCondition(subject => subject.IsSome)
//            .FailWith("but found to be None.");

//        return new AndWhichConstraint<OptionAssertions<T>, T>(this, Subject);
//    }

//    public AndConstraint<OptionAssertions<T>> BeSome(Action<T> action, string because = "", params object[] becauseArgs)
//    {
//        BeSome(because, becauseArgs);
//        Subject.IfSome(action);

//        return new AndConstraint<OptionAssertions<T>>(this);
//    }

//    public AndConstraint<OptionAssertions<T>> Be(T expected, string because = "", params object[] becauseArgs)
//    {
//        Execute.Assertion
//            .BecauseOf(because, becauseArgs)
//            .WithExpectation("Expected {context:option} to be Some {0}{reason}, ", expected)
//            .Given(() => Subject)
//            .ForCondition(subject => subject.IsSome)
//            .FailWith("but found to be None.")
//            .Then
//            .ForCondition(subject => subject.Equals(expected))
//            .FailWith("but found Some {0}.", Subject);

//        return new AndConstraint<OptionAssertions<T>>(this);
//    }
//}