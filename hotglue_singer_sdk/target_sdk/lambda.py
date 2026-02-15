import json
import pathlib
import uuid
import subprocess
import os
from logging import Logger
from typing import Union, Optional

class RealTime:
    def __init__(
            self,
            command: str,
            config: dict,
            stream_name: str,
            schema_line: str,
            record_line: str,
            logger: Logger,
            input_path: Optional[str] = None,
        ):
        self.command = command
        self.config = config
        self.stream_name = stream_name
        self.schema_line = schema_line
        self.record_line = record_line
        self.logger = logger
        self.id = str(uuid.uuid4())
        self.config_file_path = f"/tmp/{self.id}.config.json"
        self.singer_file_path = f"/tmp/{self.id}.data.singer" if not input_path else input_path
        self.state_file_path = f"/tmp/{self.id}.state.json"
        os.makedirs("/tmp", exist_ok=True)

    def _create_singer_file(self):
        if os.path.exists(self.singer_file_path):
            return

        with open(self.singer_file_path, "w") as f:
            f.writelines([
                self.schema_line + "\n",
                self.record_line
            ])

    def _create_config_file(self):
        with open(self.config_file_path, "w") as f:
            f.write(json.dumps(self.config))

    def _delete_singer_file(self):
        pathlib.Path(self.singer_file_path).unlink(missing_ok=True)

    def _delete_state_file(self):
        pathlib.Path(self.state_file_path).unlink(missing_ok=True)

    def prepare(self):
        self._create_config_file()
        self._create_singer_file()

    def run(self):
        command = f"cat {self.singer_file_path} | {self.command} --config {self.config_file_path} > {self.state_file_path}"
        self.logger.info(f"Running command: {command}")
        proc = subprocess.run(
            command,
            shell=True,
            text=True,
            capture_output=True
        )

        logs = proc.stdout.strip() or proc.stderr.strip()

        self.logger.info(logs)

        return {
            "tracebackInLogs": "Traceback" in logs,
            "logs": logs,
        }

    def get_state(self) -> Union[dict, str]:
        with open(self.state_file_path, "r") as f:
            lines = f.readlines()
            try:
                return json.loads(lines[-1].strip())
            except:
                return "".join(lines)

    def clean_up(self):
        self._delete_singer_file()
        self._delete_state_file()


def real_time_handler(
    config: dict,
    stream_name: str,
    schema_line: str,
    record_line: str,
    logger: Logger,
    input_path: Optional[str] = None,
    cli_cmd: Optional[str] = None,
):
    cli_cmd = cli_cmd or os.environ.get("CLI_CMD")
    if not cli_cmd:
        logger.info("Parameter cli_cmd or CLI_CMD env var are not set. This target does not support real time")
        raise Exception("This target does not support real time")
    logger.info(f"Entering \"real_time_handler\": cli_cmd={cli_cmd}, config={config}, stream_name={stream_name}")
    logger.info(f"Schema line: {schema_line}")
    logger.info(f"Record line: {record_line}")
    real_time = RealTime(
        cli_cmd,
        config,
        stream_name,
        schema_line,
        record_line,
        logger,
        input_path,
    )
    logger.info("Preparing files...")
    real_time.prepare()
    logger.info("Running target...")
    target_metrics = real_time.run()
    logger.info("Getting state...")
    state = real_time.get_state()
    logger.info("Cleaning up...")
    real_time.clean_up()
    logger.info("Done")
    return {
        "state": state,
        "metrics": target_metrics,
    }
