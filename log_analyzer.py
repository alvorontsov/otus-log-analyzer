import json
import pathlib
import logging
import datetime
import re
import statistics
import gzip
import string

from collections import defaultdict
from argparse import ArgumentParser

CONFIG_DEFAULT = {
    "LOG_DIR": "./logs",  # Path to dir with logs
    "REPORT_DIR": "./reports",  # Path to dir with reports
    "REPORT_SIZE": 1000,
    "LOG_FILE": None,  # File to write logs from logging module
    "ERRORS_TRESHOLD": 0.03,  # Max fraction of errors
}


def get_file_to_process(logs_dir: pathlib.Path):
    if not logs_dir.exists():
        raise FileNotFoundError("No such directory")
    last_date = None
    last_path = None
    last_ext = None
    pattern = re.compile(r"^nginx-access-ui\.log-(\d{8})(\.gz)?$")
    for path in logs_dir.iterdir():
        try:
            [(date, ext)] = re.findall(pattern, str(path).split('/')[1])
            log_date = datetime.datetime.strptime(date, "%Y%m%d").date()
            if not last_date or last_date > log_date:
                last_date = log_date
                last_ext = ext
                last_path = path
        except ValueError:
            pass
    return last_date, last_path, last_ext


def line_processing_generator(path: pathlib.Path, ext: str):
    # Open file
    if ext == '.gz':
        file = gzip.open(path.absolute(), mode='rt')
    else:
        file = path.open()
    with file:
        for line in file:
            try:
                time = float(line.split()[-1])
                parts = line.split('"')
                url = '/'.join(parts[1].split()[1].split('/')[:-1])
                yield url, time
            except Exception as exception:
                # if we cannot parse just return none
                yield None, None


def get_perc(request_times, total_count):
    return round(100. * len(request_times) / float(total_count), 3)


def get_time_sum(request_times):
    return round(sum(request_times), 3)


def get_time_perc(request_times, total_time):
    return round(100. * sum(request_times) / total_time, 3)


def get_time_avg(request_times):
    return round(statistics.mean(request_times), 3)


def get_time_max(request_times):
    return round(max(request_times), 3)


def get_time_median(request_times):
    return round(statistics.median(request_times), 3)


def get_log_stats(path: pathlib.Path, ext: str, errors_treshold: float):
    # Store stats here
    n_lines = 0
    n_fails = 0
    url2time = defaultdict(list)
    # generator function
    generator = line_processing_generator(path, ext)
    # update
    for url, time in generator:
        n_lines += 1
        if not url or not time:
            n_fails += 1
        else:
            url2time[url].append(time)
    # Check error threshold
    errors = n_fails / n_lines
    if errors > errors_treshold:
        raise Exception(f"ERROR RATE is too high {errors}")
    # Get total cnt values
    total_count = 0
    total_time = 0.
    for request_times in url2time.values():
        total_count += len(request_times)
        total_time += sum(request_times)
    # Get stats
    stats = []
    for url, request_times in url2time.items():
        stats.append({
            'url': url,
            'count': len(request_times),
            'count_perc': get_perc(request_times, total_count),
            'time_sum': get_time_sum(request_times),
            'time_perc': get_time_perc(request_times, total_time),
            'time_avg': get_time_avg(request_times),
            'time_max': get_time_max(request_times),
            "time_med": get_time_median(request_times),
        })
    return stats


def main(config):
    """
    Entrypoint
    :param config: config like CONFIG_DEFAULT above
    :return: nothing
    """
    # First, we check the logs dir and get the last log file
    logs_dir = pathlib.Path(str(config.get("LOG_DIR")))
    date, path, ext = get_file_to_process(logs_dir)
    if not date:
        raise FileNotFoundError("No logs in config LOG_DIR")
    # Then, check reports dir and if the report for our logfile already exists
    report_dir = pathlib.Path(str(config.get("REPORT_DIR")))
    if not report_dir.exists() or not report_dir.is_dir():
        raise FileNotFoundError("Invalid REPORT_DIR")
    report_filename = f'report-{date:%Y.%m.%d}.html'
    report_path = report_dir / report_filename
    if report_path.exists():
        logging.info(f"The report for log '{str(path)}' already exists")
        return
    # Finally, create and save the report
    stats = get_log_stats(path, ext, float(config.get("ERRORS_TRESHOLD")))
    stats = sorted(stats, key=lambda r: r['time_sum'], reverse=True)
    stats = stats[:config.get("REPORT_SIZE")]
    report_template_path = report_dir / "report.html"
    with report_template_path.open() as f:
        template = string.Template(f.read())
    report = template.safe_substitute(table_json=json.dumps(stats))
    with report_path.open(mode='w') as f:
        f.write(report)


if __name__ == "__main__":
    # Parse args first
    parser = ArgumentParser("Command line args parser")
    parser.add_argument("--config",
                        dest="config_path",
                        help="Path to config file")
    args = parser.parse_args()
    config = None
    # Get config from json if the corresponding arg is given, else – use default config
    if args.config_path:
        with open(args.config_path) as fp:
            config = json.load(fp)
    if not config:
        config = CONFIG_DEFAULT
    logging.basicConfig(  # type: ignore
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
        filename=config.get("LOG_FILE")
    )
    # Run with config
    try:
        main(config)
    except Exception as ex:
        logging.exception(str(ex))
