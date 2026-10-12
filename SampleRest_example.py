import requests

# The API endpoint
PROJECT_LIST_URL = 'https://api.example.invalid/endpoint'
ITEM_LIST_URL = 'https://api.example.invalid/endpoint'
CALIBRATION_URL = 'https://api.example.invalid/endpoint'
TIMESERIES_URL = 'https://api.example.invalid/endpoint'
STATISTICS_URL = 'https://api.example.invalid/endpoint'
VALUE_TYPE = ['unit8', 'int16', 'unit16', 'int32', 'unit32', 'single', 'double']

# Data to be sent
data1 = {
    "projectName": "ExampleProject"
}
data2 = {
    "projectId": 1
}

# A POST request to the API
response1 = requests.post(PROJECT_LIST_URL, json=data1)
response2 = requests.post(ITEM_LIST_URL, json=data2)

# Print the response
response1
print(response1.json())
print(response2.json())
