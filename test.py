import apache_beam as beam
import sys
from apache_beam.options.pipeline_options import PipelineOptions

# Create and set your PipelineOptions.
# For Cloud execution, specify DataflowRunner and set the Cloud Platform
# project, job name, temporary files location, and region.
# For more information about regions, check:
# https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
options = PipelineOptions(
    flags=sys.argv,
    runner='DataflowRunner',
    project='swiftflow-pipeline-poc',
    job_name='unique-job-name',
    temp_location='gs://swiftmessage-bucket',
    region='europe-west6')

# Create the Pipeline with the specified options.
# with beam.Pipeline(options=options) as pipeline:
#   pass  # build your pipeline here.

def run(argv=None):
  """Build and run the pipeline."""
  
  pipeline_options = PipelineOptions(save_main_session=True, streaming=True)
  
  with beam.Pipeline(options=pipeline_options) as p:
      
    
    _ = (
        
        p
        | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
        #| beam.Map(print)
        | 'Write to Table' >> beam.io.WriteToBigQuery(
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                        )
        
        )

if __name__ == '__main__':
  run()

