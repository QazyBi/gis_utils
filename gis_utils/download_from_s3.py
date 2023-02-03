from pathlib import Path

from minio import Minio
from fire import Fire
from pydantic import validate_arguments
from tqdm import tqdm
from functools import partial
import threading

KB = 1024
MB = 1024 * KB


def download_object(
    name,
    size,
    dst_path,
    endpoint,
    access_key,
    secret_key,
    region,
    bucket,
    object_path,
    chunk_size=2 * MB,
):
    # print("here")
    inner_client = Minio(
        endpoint=endpoint, access_key=access_key, secret_key=secret_key, region=region
    )
    downloaded = 0 * MB
    _dst_path = dst_path / "_".join(Path(name).stem.split('_')[:-1]) / Path(name).name
    _dst_path.parent.mkdir(exist_ok=True, parents=True)

    with open(_dst_path, "wb") as file_data, tqdm(
        desc=f"{_dst_path.name}",
        total=size,
        dynamic_ncols=True,
        leave=False,
        mininterval=1,
        unit="B",
        unit_scale=True,
        unit_divisor=KB,
    ) as pbar:
        while downloaded < size:
            length = (
                chunk_size if (size - downloaded) >= chunk_size else size - downloaded
            )
            data = inner_client.get_object(
                bucket,
                object_name=str(Path(object_path, Path(name).name)),
                offset=downloaded,
                length=length,
            )
            newly_downloaded = 0
            for d in data:
                newly_downloaded += file_data.write(d)
            downloaded += newly_downloaded
            pbar.update(newly_downloaded)

    if downloaded != size:
        dst_path.unlink(missing_ok=True)
        raise Exception(
            f"File error: size of '{_dst_path}' {downloaded} bytes, expected {size} bytes"
        )


@validate_arguments
def download(
    url: str,
    access_key: str,
    secret_key: str,
    object_path: str,
    dst_path: Path,
    region: str = "ru-1",
    num_workers: int = 4,
):
    """
    url - contains both endpoint and object address
    """
    endpoint = url.split("://")[0]

    client = Minio(
        endpoint=endpoint, access_key=access_key, secret_key=secret_key, region=region
    )
    bucket = "dataset"
    # object_path = "dataset_9/class_multiclass/Raster"
    # dst_path = Path("/home/qazybek/temp")
    dst_path.mkdir(exist_ok=True, parents=True)

    response = client.list_objects(bucket, prefix=object_path, recursive=True)
    arguments = [(i._object_name, i._size) for i in response]

    for i in range(0, len(arguments), num_workers):
        threads = []
        for argument in arguments[i : (i + 1) * num_workers]:
            thread = threading.Thread(
                target=partial(
                    download_object,
                    dst_path=dst_path,
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    region=region,
                    bucket=bucket,
                    object_path=object_path,
                ),
                args=(argument[0], argument[1]),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    Fire(download)
