from configparser import ConfigParser
from typing import Generator, List, Tuple
import requests
from boto3 import session as boto3_session
from defusedxml.ElementTree import fromstring as xml_fromstring
import time

config = ConfigParser()
config.read("config.ini")


class S3Fetcher:
    def __init__(self) -> None:
        self.__SESSION = boto3_session.Session()
        self.__S3 = self.__SESSION.client(
            service_name="s3",
            use_ssl=False,
            endpoint_url=str(config["S3Storage"]["API_ENDPOINT"]),
            aws_access_key_id=str(config["S3Storage"]["ACCESS_KEY"]),
            aws_secret_access_key=str(config["S3Storage"]["SECRET_KEY"]),
            config=boto3_session.Config(signature_version="s3"),
        )

    def fetch_files(self, bucket: str) -> List[str]:
        files = self.__S3.list_objects_v2(Bucket=bucket, MaxKeys=10000)
        return [file["Key"] for file in files["Contents"]]


class SelectelAPI:
    def __init__(self) -> None:
        self.__FINAL_ENDPOINT = f'{config["SelectelAPI"]["BASE_API_PATH"]}{config["SelectelAPI"]["CDN_API_PATH"]}' + config[
            "SelectelAPI"
        ][
            "PREFETCH_API_PATH"
        ].format(
            PROJECT_ID=config["SelectelAPI"]["PROJECT_ID"],
            RESOURCE_ID=config["SelectelAPI"]["RESOURCE_ID"],
        )

    def prefetch(self, files: List[str]) -> None:
        response = requests.put(
            self.__FINAL_ENDPOINT,
            json={"paths": files},
            headers={"X-token": config["SelectelAPI"]["TOKEN"]},
        )
        print(
            f"Prefetched {len(files)} files; response: {response.status_code} with {response.content}"
        )


class Utils:
    @staticmethod
    def get_files_for_prefetching(
        files: List[str],
    ) -> Tuple[List[List[str]], List[str]]:
        files = [file for file in files if file.startswith("hls/")]
        multiFiles, singleFiles = [], files
        EXTENSIONS_TO_PREFETCH = config["FilesFilter"][
            "EXTENSIONS_MULTIPLE_PREFETCH"
        ].split(",")
        EXTENSIONS_TO_PREFETCH = (
            EXTENSIONS_TO_PREFETCH[:-1]
            if EXTENSIONS_TO_PREFETCH[-1] == ""
            else EXTENSIONS_TO_PREFETCH
        )
        for file in files:
            if file.endswith(tuple(EXTENSIONS_TO_PREFETCH)):
                multiFiles.append(file)
                singleFiles.remove(file)
        return (
            list(
                Utils.split_in_chunks_of(
                    Utils.add_slash_at_the_start(sorted(multiFiles)),
                    int(config["FilesFilter"]["MULTIPLE_PREFETCH_MAX_AMOUNT"]),
                )
            ),
            Utils.add_slash_at_the_start(sorted(singleFiles)),
        )

    @staticmethod
    def split_in_chunks_of(
        files: List[str], size: int
    ) -> Generator[List[str], None, None]:
        for i in range(0, len(files), size):
            yield files[i : i + size]

    @staticmethod
    def add_slash_at_the_start(files: List[str]) -> List[str]:
        return [f"/{file}" for file in files]


def main():
    _S3 = S3Fetcher()
    _SAPI = SelectelAPI()
    prefetchFiles = Utils.get_files_for_prefetching(
        _S3.fetch_files(config["S3Storage"]["BUCKET_NAME"])
    )
    TIMES_TO_REPEAT_MULTIPLE_PREFETCH = int(
        config["TimeToWait"]["REPEAT_REQUEST_MULTIPLE_TIMES"]
    )
    for files in prefetchFiles[0]:
        print(files)
        for _ in range(TIMES_TO_REPEAT_MULTIPLE_PREFETCH):
            # _SAPI.prefetch(files)
            time.sleep(int(config["TimeToWait"]["MULTIPLE_PREFETCH_SECONDS"]))
    for files in prefetchFiles[1]:
        print(files)
        # _SAPI.prefetch([files])
        time.sleep(int(config["TimeToWait"]["SINGLE_PREFETCH_SECONDS"]))


if __name__ == "__main__":
    main()
