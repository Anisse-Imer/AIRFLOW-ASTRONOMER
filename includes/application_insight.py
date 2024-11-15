"""
Query the Azure App Insights data from Python and convert to dictionaries/pandas data frames
Example:
> AZURE_APPLICATION_INSIGHTS_ID = "your-application-id"
> AZURE_APPLICATION_INSIGHTS_KEY = "your-api-key-from-the-app-insights-portal"
> client = AppInsightsClient(AZURE_APPLICATION_INSIGHTS_ID, AZURE_APPLICATION_INSIGHTS_KEY)
> df = client.query_as_df(f'pageViews | where name == "sdfdsf" and operation_Name == "/page/1" | summarize count() by client_CountryOrRegion')
"""
# Anisse : took this code from a github repo, going to adapt it.
import requests
import pandas as pd
from jinja2 import Template
from msal import ConfidentialClientApplication

class AppInsightsClientBuilder:
    # Inputs stored
    AAD_id:str
    AAD_secret:str
    scopes:list
    AzureAuthorityId:str
    ApplicationInsights_ID:str

    def __init__(self, AAD_id:str, AAD_secret:str, AzureAuthorityId:str, ApplicationInsights_ID:str, scopes:list=['https://api.applicationinsights.io/.default']):
        self.AAD_id = AAD_id
        self.AAD_secret = AAD_secret
        self.scopes = scopes
        self.AzureAuthorityId = AzureAuthorityId
        self.ApplicationInsights_ID:str = ApplicationInsights_ID

    def set_AAD_id(self, AAD_id:str):
        self.AAD_id = AAD_id
    def set_AAD_secret(self, AAD_secret:str):
        self.AAD_secret = AAD_secret
    def set_scopes(self, scopes:list):
        if(all(isinstance(scope, str) for scope in scopes)):
            self.scopes = scopes
    def set_AzureAuthorityId(self, AzureAuthorityId):
        self.AzureAuthorityId = AzureAuthorityId
    def set_ApplicationInsights_ID(self, ApplicationInsights_ID:str):
        self.ApplicationInsights_ID = ApplicationInsights_ID

    # Returns an AppInsightsClients instance
    def build(self):
        return AppInsightsClient(AAD_id=self.AAD_id, AAD_secret=self.AAD_secret, scopes=self.scopes, ApplicationInsights_ID=self.ApplicationInsights_ID, AzureAuthorityId=self.AzureAuthorityId)


class AppInsightsClient:
    # Inputs stored
    _AAD_id:str
    _AAD_secret:str
    _scopes:list
    _AzureAuthorityId:str
    # External source
    __Azure_token:str = ""
    # Calculated
    base_url:str
    session:requests.Session

    def __init__(self, AAD_id:str, AAD_secret:str, AzureAuthorityId:str, ApplicationInsights_ID:str, scopes:list=['https://api.applicationinsights.io/.default']):
        # Store the inputs
        self.AAD_id = AAD_id
        self.AAD_secret = AAD_secret
        self.AzureAuthorityId = AzureAuthorityId
        self.ApplicationInsights_ID = ApplicationInsights_ID
        # -- scopes - only contains strings
        if(all(isinstance(scope, str) for scope in scopes)):
            self.scopes = scopes
        else:
            raise Exception("scopes : only contain str type")
        self._base_url = f"https://api.applicationinsights.io/v1/apps/{self.ApplicationInsights_ID}"    
        # Get the token
        self.__Azure_token = self.get_microsoft_token()
        self.session = requests.Session()
        self.session.headers = {
            "Authorization" : f"Bearer {self.__Azure_token}",
            "Content-Type" : "application/json",
        }

    # Get the token from the auth Microsoft API - with the native or the input scopes
    def get_microsoft_token(self):
        token = ""
        # Get the microsoft token
        self.authority_url = f"https://login.microsoftonline.com/{self.AzureAuthorityId}"
        client_instance = ConfidentialClientApplication(
            client_id=self.AAD_id,
            client_credential=self.AAD_secret,
            authority=self.authority_url
        )
        result = client_instance.acquire_token_for_client(scopes=self.scopes)
        if result and result['access_token']:
            token = result['access_token']
        return token

    def _result_to_dict(self, data):
        # Convert the JSON into a list of dictionaries
        # App Insights API will return a list of result sets
        # Each result set defines a name, the columns and the rows
        # Convert to a list of dictionaries with the key as the column name
        result = {}
        for table in data['tables']:
            result[table['name']] = []
            for row in table['rows']:
                d = {table['columns'][i]['name']: x for i, x in enumerate(row)}
                result[table['name']].append(d)
        return result

    def _result_to_df(self, data):
        # Convert the JSON into a dictionary of pandas dataframes
        result = {}
        for table in data['tables']:
            result[table['name']] = pd.DataFrame.from_records(table['rows'], columns=[col['name'] for col in table['columns']])
        return result

    def _query(self, query):
        resp = self.session.get(self._base_url + "/query", params={'query': query})
        resp.raise_for_status()
        return resp.json()

    def query_as_dict(self, query, primary_result=True):
        result = self._query(query)
        if primary_result:
            return self._result_to_dict(result)['PrimaryResult']
        return self._result_to_dict(result)

    def query_as_df(self, query, primary_result=True):
        result = self._query(query)
        if primary_result:
            return self._result_to_df(result)['PrimaryResult']
        return self._result_to_df(result)
    
    def error_size(respJson) -> bool:
        if "error" in respJson.keys():
            for detail in respJson["error"]["details"]:
                if detail["innererror"]["code"] == "-2133196797":
                    return True
        else:
            return False

    def query_as_df_incremental(self, query_template:Template, params:dict={}) -> pd.DataFrame:
        if not ("StartDate" in params and "EndDate" in params):
            raise Exception("application_insight.py : AppInsightsClient : startDate and endDate parameters must be set")
        query = query_template.render(**params)
        resp = self.session.get(self._base_url + "/query", params={'query': query})
        respJson = resp.json()
        if resp.status_code == 200 and respJson:
            if AppInsightsClient.error_size(respJson=respJson):
                del respJson
                # Find the difference between the two dates
                date_diff = params["EndDate"] - params["StartDate"]
                # Date between these two
                middle_date = params["StartDate"] + (date_diff / 2)
                # Define the new timestamp parameters
                params1 = params.copy()
                params2 = params.copy()       
                params1["EndDate"] = middle_date
                params2["StartDate"] = middle_date
                # Call the other 
                df1 = self.query_as_df_incremental(query_template=query_template, params=params1)
                df2 = self.query_as_df_incremental(query_template=query_template, params=params2)
                return pd.concat([df1, df2])
            else:
                return self._result_to_df(respJson)['PrimaryResult']
        elif resp.status_code == 400:
            raise Exception(f"application_insight.py : AppInsightsClient : bad query : {resp.content}")
        elif resp.status_code == 403:
            raise Exception("application_insight.py : AppInsightsClient : authorization token is wrong")
    
    def query_as_df_date(self, query_template:Template, params:dict={}):
        pass