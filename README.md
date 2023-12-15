# DR2 Ingest Parsed Court Document Event Handler

The lambda does the following.

* Reads a TRE input message from an SQS message
* Downloads the package from TRE.
* Unzips it
* Untars it
* Uploads all files from the package to S3 with a UUID. This includes files we don't care about but it's easier than trying to parse json on the fly
* Parses the metadata json from the package.
* Gets a series and department code from a static Map, based on the court.
* Generates the bagit files and uploads them to S3 in memory.
* Copies the docx and metadata files into the `data/` directory
* Starts a step function execution with the judgment details. 

The department and series lookup is very judgment-specific but this can be changed if we start taking in other transfers.

## Metadata Mapping
This table shows how we map the metadata from TRE to our bagit json.  
Each field in a row is tried, if it's not null, it's used, otherwise the next field is tried.

The input to the lambda is an SQS event.  
This is an example of the event body.

```json
{
  "parameters": {
    "status": "status",
    "reference": "TEST-REFERENCE",
    "s3Bucket": "inputBucket",
    "s3Key": "test.tar.gz"
  }
}
```

We can also replay the message with the `skipSeriesLookup` parameter

```json
{
  "parameters": {
    "status": "status",
    "reference": "TEST-REFERENCE",
    "skipSeriesLookup": true,
    "s3Bucket": "inputBucket",
    "s3Key": "test.tar.gz"
  }
}
```

The lambda doesn't produce an output. It writes files and metadata to S3 in the Bagit format. 
The Bagit metadata is split into two files:

#### metadata.json
```json
[
  {
    "id_Code": "cite",
    "id_Cite": "cite",
    "id_URI": "https://example.com/id/court/2023/",
    "id": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "parentId": null,
    "title": "test",
    "type": "ArchiveFolder",
    "name": "https://example.com/id/court/2023/"
  },
  {
    "originalFiles": [
      "c7e6b27f-5778-4da8-9b83-1b64bbccbd03"
    ],
    "originalMetadataFiles": [
      "61ac0166-ccdf-48c4-800f-29e5fba2efda"
    ],
    "description": "test",
    "id_UpstreamSystemReference": "TEST-REFERENCE",
    "id_URI": "https://example.com/id/court/2023/",
    "id_NeutralCitation": "cite",
    "id": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "parentId": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "title": "Test.docx",
    "type": "Asset",
    "name": "Test.docx"
  },
  {
    "id": "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "Test",
    "type": "File",
    "name": "Test.docx",
    "sortOrder": 1,
    "fileSize": 15684
  },
  {
    "id": "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "",
    "type": "File",
    "name": "TRE-TEST-REFERENCE-metadata.json",
    "sortOrder": 2,
    "fileSize": 215
  }
]
```

#### bag-info.json
```json
{
  "id_ConsignmentReference": "test-identifier",
  "id_UpstreamSystemReference": "TEST-REFERENCE",
  "transferringBody": "test-organisation",
  "transferCompleteDatetime": "2023-10-31T13:40:54Z",
  "upstreamSystem": "TRE: FCL Parser workflow",
  "digitalAssetSource": "Born Digital",
  "digitalAssetSubtype": "FCL"
}

```



[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments/blob/main/ingest_parsed_court_document_event_handler.tf)

## Environment Variables

| Name          | Description                                                               |
|---------------|---------------------------------------------------------------------------|
| OUTPUT_BUCKET | The raw cache bucket for storing the bagit package created by this lambda |
| SFN_ARN       | The arn of the step function this lambda will trigger.                    |
