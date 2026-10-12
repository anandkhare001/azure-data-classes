from Measure import AzureDataExplorer


def get_turbine_platform(platFormType, cn=None):
    if platFormType == 'CategoryA':
        query = "ExampleAssetTable" \
            " | where LocationCategory == 'CategoryA' and isnotempty(AssetPlatform)" \
            " | distinct AssetPlatform"
    elif platFormType == 'CategoryB':
        query = "ExampleAssetTable" \
            " | where LocationCategory == 'CategoryB' and isnotempty(AssetPlatform)" \
            " | distinct AssetPlatform"
    else:
        query = "ExampleAssetTable" \
            " | where LocationCategory !in ('CategoryA', 'CategoryB') and isnotempty(AssetPlatform)" \
            " | distinct AssetPlatform"

    if cn is None:
        cn = AzureDataExplorer.AzureDataExplorer()
    platFormListDf = cn.query_from_adx('ExampleAssetDatabase', query)
    platFormList = platFormListDf['AssetPlatform'].tolist()
    return platFormList


def get_turbine_type(turPlatForm, cn=None):
    query = f"""
            ExampleAssetTable
            | where AssetPlatform == '{turPlatForm}'
            | project AssetModel, SiteName
            """
    if cn is None:
        cn = AzureDataExplorer.AzureDataExplorer()
    turbineTypeListDf = cn.query_from_adx('ExampleAssetDatabase', query)
    return turbineTypeListDf


def get_turbine_park_name(turbineType, cn=None):
    query = f"""
            ExampleAssetTable
            | where AssetModel == '{turbineType}'
            | project SiteName
            """
    if cn is None:
        cn = AzureDataExplorer.AzureDataExplorer()
    parkListDf = cn.query_from_adx('ExampleAssetDatabase', query)
    return parkListDf


def get_turbine_name(turbineParkName, cn=None):
    query = f"""    
            ExampleAssetTable
            | where SiteName == '{turbineParkName}'
            | project AssetName
            """
    if cn is None:
        cn = AzureDataExplorer.AzureDataExplorer()
    turNameList = cn.query_from_adx('ExampleAssetDatabase', query)
    return turNameList


def get_metric_info(cn=None):
    query = f"""
            ExampleSignalTable
            | project SignalId, SignalName, MeasurementUnit, SignalDescription
            """
    if cn is None:
        cn = AzureDataExplorer.AzureDataExplorer()
    metricListDf = cn.query_from_adx('ExampleMetadataDatabase', query)
    return metricListDf


if __name__ == "__main__":
    a = get_turbine_platform("CategoryA")
    print(a[0])
    b = get_metric_info()
    print(b.SignalId.values)
    c = get_turbine_type('ExamplePlatform')
    print(c.AssetModel.values)
    d = get_turbine_park_name('ExampleModel')
    print(d.SiteName[0])
    e = get_turbine_name('ExampleSite')
    print(e.AssetName[0])
