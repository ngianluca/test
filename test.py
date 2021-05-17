import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Create and set your PipelineOptions.
# For Cloud execution, specify DataflowRunner and set the Cloud Platform
# project, job name, temporary files location, and region.
# For more information about regions, check:
# https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
options = PipelineOptions(
    flags=argv,
    runner='DataflowRunner',
    project='swiftflow-pipeline-poc',
    job_name='unique-job-name',
    temp_location='gs://swiftmessage-bucket',
    region='europe-west6')

# Create the Pipeline with the specified options.
# with beam.Pipeline(options=options) as pipeline:
#   pass  # build your pipeline here.

def run():
    pass

if __name__ == "__main__":
    run()
