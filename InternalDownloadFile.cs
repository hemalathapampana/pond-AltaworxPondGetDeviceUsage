private void InternalDownloadFile(string path, Stream output, SftpDownloadAsyncResult asyncResult, Action<ulong> downloadCallback)
{
    if (output == null)
    {
        throw new ArgumentNullException("output");
    }

    if (path.IsNullOrWhiteSpace())
    {
        throw new ArgumentException("path");
    }

    if (_sftpSession == null)
    {
        throw new SshConnectionException("Client not connected.");
    }

    string canonicalPath = _sftpSession.GetCanonicalPath(path);
    using ISftpFileReader sftpFileReader = base.ServiceFactory.CreateSftpFileReader(canonicalPath, _sftpSession, _bufferSize);
    ulong num = 0uL;
    while (asyncResult == null || !asyncResult.IsDownloadCanceled)
    {
        byte[] array = sftpFileReader.Read();
        if (array.Length == 0)
        {
            break;
        }

        output.Write(array, 0, array.Length);
        num += (ulong)array.Length;
        if (downloadCallback != null)
        {
            ulong downloadOffset = num;
            ThreadAbstraction.ExecuteThread(delegate
            {
                downloadCallback(downloadOffset);
            });
        }
    }
}