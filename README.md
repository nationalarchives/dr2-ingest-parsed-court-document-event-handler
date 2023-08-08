# DR2 Ingest Parsed Court Document Event Handler

The lambda does the following.

* Reads a TRE input message from an SQS message
* Downloads the package from TRE.
* Unzips it
* Untars it
* Uploads all files from the package to S3 with a UUID. This includes files we don't care about but it's easier than trying to parse json on the fly
* Parses the metadata json from the package.
* Gets a series and department code from a static Map, based on the cite.
* Generates the bagit files and uploads them to S3 in memory.
* Copies the docx and metadata files into the `data/` directory
* Starts a step function execution with the judgment details. 

The department and series lookup is very judgment-specific but this can be changed if we start taking in other transfers.


[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments/blob/main/ingest_parsed_court_document_event_handler.tf)

## Environment Variables

| Name          | Description                                                               |
|---------------|---------------------------------------------------------------------------|
| OUTPUT_BUCKET | The raw cache bucket for storing the bagit package created by this lambda |
| SFN_ARN       | The arn of the step function this lambda will trigger.                    |
