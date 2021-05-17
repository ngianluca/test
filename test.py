import logging
import apache_beam as beam

PROJECT = "swiftflow-pipeline-poc"
BUCKET = "swiftmessage-bucket"
schema = "event:NULLABLE"
FIELD_NAMES = ["foo"]


class CsvToDictFn(beam.DoFn):
    def process(self, element):
        return [dict(zip(FIELD_NAMES, element.split(",")))]


def run():
    argv = [
        "--project={0}".format(PROJECT),
        "--staging_location=gs://{0}/staging/".format(BUCKET),
        "--temp_location=gs://{0}/staging/".format(BUCKET),
        "--runner=DataflowRunner",
        "--max_num_workers=2",
        "--save_main_session",
        "--experiments=use_beam_bq_sink"
    ]

    p = beam.Pipeline(argv=argv)

    data = ['{0},good_line_{1}'.format(i + 1, i + 1) for i in range(10)]
    data.append('this is a bad row')

    events = (p
        | "Create data" >> beam.Create(data)
        | "CSV to dict" >> beam.ParDo(CsvToDictFn())
        | "Write results" >> beam.io.gcp.bigquery.WriteToBigQuery(
            "{0}:dataflow_test.good_lines".format(PROJECT),
            schema=schema,
            method='STREAMING_INSERTS'
        )
    )

    (events[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
        | "Bad lines" >> beam.io.textio.WriteToText("gs://{0}/error_log.txt".format(BUCKET)))

    p.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
