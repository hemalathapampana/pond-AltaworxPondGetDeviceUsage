using System;

namespace Amop.Core.Models.Settings
{
    public interface ISettingsRepository
    {
        GeneralProviderSettings GetGeneralProviderSettings();
        OptimizationSettings GetOptimizationSettings(int? tenantId = null);
        JasperProviderSettings GetJasperDeviceSettings(int serviceProviderId);
        TelegenceProviderSettings GetTelegenceDeviceSettings(int serviceProviderId);
        eBondingProviderSettings GetEbondingDeviceSettings(int serviceProviderId);
        PondProviderSettings GetPondDeviceSettings(int serviceProviderId, Action<string, string> logFunction);
    }
}
