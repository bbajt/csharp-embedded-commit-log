namespace ByTech.EmbeddedCommitLog;

/// <summary>
/// Represents the outcome of a fallible PECL operation: either a success value of type
/// <typeparamref name="T"/> or a failure described by <typeparamref name="TError"/>.
/// </summary>
/// <typeparam name="T">The success value type.</typeparam>
/// <typeparam name="TError">The failure value type.</typeparam>
/// <remarks>
/// Construct instances via <see cref="Ok"/> and <see cref="Fail"/>.
/// Always check <see cref="IsSuccess"/> (or <see cref="IsFailure"/>) before accessing
/// <see cref="Value"/> or <see cref="Error"/> to avoid <see cref="InvalidOperationException"/>.
/// </remarks>
public readonly struct Result<T, TError>
{
    private readonly T _value;
    private readonly TError _error;
    private readonly bool _isSuccess;

    private Result(T value, TError error, bool isSuccess)
    {
        _value = value;
        _error = error;
        _isSuccess = isSuccess;
    }

    /// <summary>Returns <see langword="true"/> when the operation succeeded.</summary>
    public bool IsSuccess => _isSuccess;

    /// <summary>Returns <see langword="true"/> when the operation failed.</summary>
    public bool IsFailure => !_isSuccess;

    /// <summary>
    /// The success value.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <see cref="IsSuccess"/> is <see langword="false"/>.
    /// Check <see cref="IsSuccess"/> before accessing this property.
    /// </exception>
    public T Value => _isSuccess
        ? _value
        : throw new InvalidOperationException("Cannot access Value on a failed Result.");

    /// <summary>
    /// The failure description.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <see cref="IsSuccess"/> is <see langword="true"/>.
    /// Check <see cref="IsFailure"/> before accessing this property.
    /// </exception>
    public TError Error => !_isSuccess
        ? _error
        : throw new InvalidOperationException("Cannot access Error on a successful Result.");

    /// <summary>Creates a successful result wrapping <paramref name="value"/>.</summary>
    public static Result<T, TError> Ok(T value) => new(value, default!, true);

    /// <summary>Creates a failed result wrapping <paramref name="error"/>.</summary>
    public static Result<T, TError> Fail(TError error) => new(default!, error, false);
}
