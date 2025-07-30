import boto3
import time
from datetime import datetime

PARSE_ERROR_LOG_GROUP = "sbm-ingester-parse-error-log"
RUNTIME_ERROR_LOG_GROUP = "sbm-ingester-runtime-error-log"
ERROR_LOG_GROUP = "sbm-ingester-error-log"
EXECUTION_LOG_GROUP = "sbm-ingester-execution-log"
METRICS_LOG_GROUP = "sbm-ingester-metrics-log"
BUCKET_NAME = "sbm-file-ingester"
PARSE_ERR_DIR = "newParseErr/"
IRREVFILES_DIR = "newIrrevFiles/"
PROCESSED_DIR = "newP/"

class CloudWatchLogger:
    def __init__(self, log_group: str, region_name: str = "ap-southeast-2"):
        self.log_group = log_group
        self.client = boto3.client("logs", region_name=region_name)
        self.sequence_token = None
        self.current_stream = None
        self._update_stream()

    def _get_daily_stream_name(self) -> str:
        """Return today's log stream name (UTC)."""
        return datetime.utcnow().strftime("day-%Y-%m-%d")

    def _update_stream(self):
        """Ensure we're using the correct log stream for today."""
        stream_name = self._get_daily_stream_name()
        if stream_name != self.current_stream:
            # New day or first initialization
            self.current_stream = stream_name
            self.sequence_token = None
            self._ensure_stream(stream_name)

    def _ensure_stream(self, stream_name: str):
        """Create the log stream if it doesn't exist."""
        try:
            self.client.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=stream_name
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass

    def log(self, message: str):
        self._update_stream()

        timestamp = int(round(time.time() * 1000))
        kwargs = {
            "logGroupName": self.log_group,
            "logStreamName": self.current_stream,
            "logEvents": [{"timestamp": timestamp, "message": message}]
        }
        if self.sequence_token:
            kwargs["sequenceToken"] = self.sequence_token

        response = self.client.put_log_events(**kwargs)
        self.sequence_token = response.get("nextSequenceToken")