using System;
using System.Net.Http;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Microsoft.Data.SqlClient;
using Polly;
using StackExchange.Redis;

namespace Amop.Core.Resilience
{
    public class PolicyFactory : IPolicyFactory
    {
        public const int RetryBaseSeconds = 2;

        private readonly IKeysysLogger logger;

        public PolicyFactory(IKeysysLogger logger)
        {
            this.logger = logger;
        }

        public IAsyncPolicy GetHttpRetryPolicy(int maxRetries)
        {
            return Policy
                .Handle<HttpRequestException>()
                .Or<TimeoutException>()
                .WaitAndRetryAsync(maxRetries, CalculateRetryDelay, LogRetryAttempt);
        }

        public IAsyncPolicy GetSqlAsyncRetryPolicy(int maxRetries)
        {
            return Policy
                .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                .Or<TimeoutException>()
                .WaitAndRetryAsync(maxRetries, CalculateRetryDelay, LogRetryAttempt);
        }

        public ISyncPolicy GetSqlRetryPolicy(int maxRetries)
        {
            return Policy
                .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                .Or<TimeoutException>()
                .WaitAndRetry(maxRetries, CalculateRetryDelay, LogRetrySQLAttempt);
        }

        public ISyncPolicy<bool> GetRedisCacheRetryPolicy(int maxRetries)
        {
            var policyBuilder = Policy
                                .HandleResult<bool>(result => !result)
                                .Or<Exception>();
            return
                policyBuilder.WaitAndRetry(maxRetries,
                            sleepDurationProvider: (retryCount, result, ctx) =>
                            {
                                return CalculateRetryDelay(retryCount);
                            },
                            onRetry: (result, timeSpan, retryCount, ctx) =>
                            {
                                logger.LogInfo("WARNING",
                                    $"Encountered transient error. Delaying for {timeSpan.TotalMilliseconds}ms, then making retry {retryCount}. Current Result: {(result.Exception != null ? result.Exception?.Message : result.Result.ToString())}");
                            }
                            );
        }

        public ISyncPolicy<RedisValue> GetRedisCacheFetchRetryPolicy(int maxRetries)
        {
            var policyBuilder = Policy
                                .HandleResult<RedisValue>(result => {
                                    byte[] resultBytes = result;
                                    return result.IsNull || resultBytes == null || resultBytes.Length == 0;
                                })
                                .Or<Exception>();
            return
                policyBuilder.WaitAndRetry(maxRetries,
                            sleepDurationProvider: (retryCount, result, ctx) =>
                            {
                                return CalculateRetryDelay(retryCount);
                            },
                            onRetry: (result, timeSpan, retryCount, ctx) =>
                            {
                                logger.LogInfo("WARNING",
                                    $"Encountered transient error. Delaying for {timeSpan.TotalMilliseconds}ms, then making retry {retryCount}. Current Result: {(result.Exception != null ? result.Exception?.Message : result.Result.ToString())}");
                            }
                            );
        }
        public ISyncPolicy SqlRetryPolicy(int maxRetries)
        {
            return Policy
                .Handle<SqlException>()
                .Or<TimeoutException>()
                .Or<Exception>()
                .WaitAndRetry(maxRetries, CalculateRetryDelay, (Exception exception, TimeSpan timeSpan, int retryCount, Context context) => {
                    logger.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.RETRY_LOG_FORMAT, retryCount, maxRetries, timeSpan.TotalSeconds));
                });
        }

        private static TimeSpan CalculateRetryDelay(int retryAttempt)
        {
            return TimeSpan.FromSeconds(Math.Pow(RetryBaseSeconds, retryAttempt));
        }

        private void LogRetryAttempt(Exception exception, TimeSpan timeSpan, int retryCount, Context context)
        {
            logger.LogInfo("WARNING",
                $"Encountered transient error. Delaying for {timeSpan.TotalMilliseconds}ms, then making retry {retryCount}. Exception: {exception?.Message}");
        }

        private void LogRetrySQLAttempt(Exception exception, TimeSpan timeSpan, int retryCount, Context context)
        {
            logger.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.SQL_ERROR_RETRY_MESSAGE, timeSpan.TotalMilliseconds, retryCount));
        }
    }
}
