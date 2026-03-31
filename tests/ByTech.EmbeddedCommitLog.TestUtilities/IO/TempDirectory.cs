namespace ByTech.EmbeddedCommitLog.TestUtilities.IO;

/// <summary>
/// Creates a unique temporary directory on construction and deletes it (recursively)
/// on disposal. Suitable for use as an xUnit <c>IDisposable</c> fixture or a local
/// using-scoped variable in test methods.
/// </summary>
public sealed class TempDirectory : IDisposable
{
    private bool _disposed;

    /// <summary>Gets the absolute path of the temporary directory.</summary>
    public string Path { get; }

    /// <summary>
    /// Creates a new uniquely-named temporary directory under <see cref="System.IO.Path.GetTempPath"/>.
    /// </summary>
    public TempDirectory()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            System.IO.Path.GetRandomFileName());

        Directory.CreateDirectory(Path);
    }

    /// <summary>Deletes the temporary directory and all its contents.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (Directory.Exists(Path))
        {
            Directory.Delete(Path, recursive: true);
        }
    }
}
