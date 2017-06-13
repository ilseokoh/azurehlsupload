[한국어 버전](README-kor.md)

# Upload HLS Streaming contents to Azure Blob Storage and request CDN pre-load with Azure Functions

This application upload HLS streaming contents to Azure Blob Storage using Azure Functions and request CDN pre-loading using Azure CDN REST API. 

## Development Tools 

 - [Visual Studio 2017 Preview](https://www.visualstudio.com/vs/preview/) (2017-06-13)
 - [Azure Funtions Tools for Visual Studio 2017](https://marketplace.visualstudio.com/items?itemName=AndrewBHall-MSFT.AzureFunctionToolsforVisualStudio2017)
 - C#

## Diagram

[Block Diagram](images/upload-diagram.png)

## Process

1. Request upload item to /api/upload API (RequestUploadFunction)
1. Parse HLS URL to recognize all the files need to copy and insert Azure Queue with 10 items (M3u8UrlParseFunction)
1. ContentUploaderFunction tirgger by Queue and upload 10 items to Azure blob storage (ContentUploaderFunction)
1. Monitor the progress using /api/monitor and /api/status API (MonitorUploadFunction, UploadStatusFunction)

## API Spec

Code runs on Azure App Services. 

### 업로드 요청

- Request URL: http://{app-service-name}.azurewebsites.net/api/upload?code={secretcode}}
- Method: POST
- Header
    - Content-Type: application/json
- Body
    - primaryUrl : Main playlist URL
    - secondaryUrls: Secondary playlist URL
- Request
```json
{
   "primaryUrl": "https://{hostname}/playlist_1080.m3u8",
    "secondaryUrls" : [
        "https://{hostname}/playlist_720.m3u8",
        "https://{hostname}/playlist_480.m3u8",
        "https://{hostname}/playlist_360.m3u8",
        "https://{hostname}/playlist_300.m3u8"
    ]
}
```

- Response: Success (200)
```json
{
   "status": "success",
   "data": "",
   "message": "requested: http://{hostname}/playlist_1080.m3u8"
}
```

- Response: Failed (500)
```json
{
   "status": "error",
   "data": "{request body}",
   "message": "{error message}"
}
```

### Status 

- Request URL: http://{app-service-name}.azurewebsites.net/api/status?code={secret-code}
- Method: POST
- Body: primaryUrl by string (not json)
- Response 성공(200)
```json
{
   "url": " http://{hostname}/playlist_1080.m3u8",
   "fileCount": 3271,
   "completeCount": 866,
   "progress": 0.2647508407214918985019871599,
   "hasError": false
}
```
<table>
<tr>
<td>fileCount</td>
<td>Total file count</td>
</tr>
<tr>
<td>completeCount</td>
<td>Complete Count</td>
</tr>
<tr>
<td>progress</td>
<td>(fileCount / complete)</td>
</tr>
</table>

### Monitor

- Request URL: http://{app-service-name}.azurewebsites.net/api/monitor?code={secretcode}
- Method: GET
- Response: Success(200)
```json
{
  "totalCount": 9,
  "errorCount": 1,
  "ongoingCount": 3,
  "ongoingList": [
    {
      "url": "http://odkcdn1.azureedge.net/72sec/playlist_1080.m3u8",
      "fileCount": 3457,
      "completeCount": 902,
      "progress": 0.2609198727220133063349725195,
      "hasError": false
    },
    {
      "url": "http://odkcdn1.azureedge.net/infinite-challenge/infinite-challenge-e528/playlist_720.m3u8",
      "fileCount": 3271,
      "completeCount": 891,
      "progress": 0.2723937633751146438398043412,
      "hasError": false
    },
    {
      "url": "http://odkcdn1.azureedge.net/youns-kitchen/youns-kitchen-e7/playlist_720.m3u8",
      "fileCount": 3589,
      "completeCount": 1521,
      "progress": 0.4237949289495681248258567846,
      "hasError": true
    }
  ],
  "errorList": [
      {
      "url": "http://odkcdn1.azureedge.net/youns-kitchen/youns-kitchen-e7/playlist_720.m3u8",
      "fileCount": 3589,
      "completeCount": 1521,
      "progress": 0.4237949289495681248258567846,
      "hasError": true
    }
  ]
}
```
<table>
<tr>
    <td>totalCount</td>
    <td>Total Content count</td>
</tr>
<tr>
    <td>errorCount</td>
    <td>Content count that has error</td>
</tr>
<tr>
    <td>ongoingCount</td>
    <td>Content count that is uploading</td>
</tr>
<tr>
    <td>ongoingList</td>
    <td>Detail list of uploading content</td>
</tr>
<tr>
    <td>errorList</td>
    <td>Detail List of error content</td>
</tr>
</table>
- Response: Failed (500)

## CDN Pre-load

Azure CDN can be set the origin using Azure blob storage. But uploaded contents won't cache on CDN immediately. One or more requests of the content need to cache them. Pre-load request is very useful to improve CDN performance. Azure CDN pre-load feature has limit, 10 urls per minute, and it will take 2-3 minutes to finish pre-load. So the code request only 100 seconds of the content. 

#### Reference for Azure CDN Pre-load 
 - ["Use portal to create an Azure Active Directory application and service principal that can access resources"](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal)
- ["Get started with Azure CDN development"](https://docs.microsoft.com/en-us/azure/cdn/cdn-app-dev-net) 